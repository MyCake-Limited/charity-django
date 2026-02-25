import csv
import io
import logging
import random
from datetime import datetime, timedelta

import psycopg2.extras
import requests
import requests_cache
from django.core.management.base import BaseCommand
from django.db import connections, router, transaction
from django.utils.text import slugify

from charity_django.ccni.models import (
    Charity,
    CharityClassification,
    ClassificationTypes,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = "Import CCNI data from a zip file"
    page_size = 10_000

    base_url = "https://www.charitycommissionni.org.uk/api/charity-search/exportSearchResultsToCsv/?homePageId=c0884e5e-b621-4563-a378-2b313e15c74a&pageNumber=1&sortOption=Rank%3Basc&searchText="

    def _get_db(self):
        return router.db_for_write(Charity)

    def _get_connection(self):
        return connections[self._get_db()]

    def logger(self, message, error=False):
        if error:
            logger.error(message)
            return
        logger.info(message)

    def add_arguments(self, parser):
        parser.add_argument("--sample", type=int, default=0)

    def handle(self, *args, **options):
        self.session = requests_cache.CachedSession(
            "demo_cache.sqlite",
            expire_after=timedelta(days=1),
        )
        self.sample = options.get("sample")

        self.charities = []
        self.charity_classification = set()

        self.fetch_file()

    def fetch_file(self):
        self.files = {}
        try:
            r = self.session.get(self.base_url, verify=True)
        except requests.exceptions.SSLError:
            r = self.session.get(self.base_url, verify=False)
        r.raise_for_status()

        file = io.StringIO(r.content.decode("latin1"))
        reader = csv.DictReader(file)
        for k, row in enumerate(reader):
            self.add_charity(row)
        self.save_charities()

    def add_charity(self, record):
        record = {
            slugify(k).replace("-", "_").replace("__", "_").replace("__", "_"): (
                v if v != "" else None
            )
            for k, v in record.items()
            if k
        }

        # date fields
        for k, format_ in [
            ("date_registered", "%d/%m/%Y"),
            ("date_for_financial_year_ending", "%d %B %Y"),
            ("financial_period_start", "%d %B %Y"),
            ("financial_period_end", "%d %B %Y"),
        ]:
            if record.get(k):
                record[k] = datetime.strptime(record[k], format_).date()

        # int fields
        for k in [
            "reg_charity_number",
            "sub_charity_number",
            "total_income",
            "total_spending",
            "charitable_spending",
            "income_generation_and_governance",
            "retained_for_future_use",
            "total_income_previous_financial_period",
            "employed_staff",
            "uk_and_ireland_volunteers",
            "income_from_donations_and_legacies",
            "income_from_charitable_activities",
            "income_from_other_trading_activities",
            "income_from_investments",
            "income_from_other",
            "total_income_and_endowments",
            "expenditure_on_raising_funds",
            "expenditure_on_charitable_activities",
            "expenditure_on_governance",
            "expenditure_on_other",
            "total_expenditure",
            "assets_and_liabilities_total_fixed_assets",
            "total_net_assets_and_liabilities",
        ]:
            if record.get(k) is not None:
                record[k] = int(record[k])

        # array fields
        for k in [
            "what_the_charity_does",
            "who_the_charity_helps",
            "how_the_charity_works",
        ]:
            record[k] = record.get(k, "").split(",")
            for classification_record in record[k]:
                if classification_record.strip():
                    self.charity_classification.add(
                        (
                            record["reg_charity_number"],
                            ClassificationTypes[k.upper()],
                            classification_record.strip(),
                        )
                    )

        # company number field
        if record.get("company_number") == "0":
            record["company_number"] = None
        if record.get("company_number"):
            record["company_number"] = "NI" + record["company_number"].zfill(6)

        # website field
        if record.get("website") and not record.get("website").startswith("http"):
            record["website"] = "http://" + record["website"]
        self.charities.append(record)

    def _execute_many(self, cursor, statement, values):
        connection = self._get_connection()
        if connection.vendor == "postgresql":
            psycopg2.extras.execute_values(
                cursor,
                statement,
                values,
                page_size=self.page_size,
            )
        else:
            cursor.executemany(
                statement,
                values,
            )

    def save_charities(self):
        db = self._get_db()
        connection = connections[db]
        with connection.cursor() as cursor, transaction.atomic(using=db):
            if self.sample:
                self.logger("Sampling {:,.0f} charities".format(self.sample))
                self.logger(
                    "Population of {:,.0f} charities".format(len(self.charities))
                )
                self.charities = random.sample(self.charities, self.sample)
                charity_numbers = [c["reg_charity_number"] for c in self.charities]
                self.charity_classification = set(
                    [c for c in self.charity_classification if c[0] in charity_numbers]
                )
                self.logger("Sampled {:,.0f} charities".format(len(self.charities)))

            for object in [Charity, CharityClassification]:
                # delete existing charities
                self.logger(
                    "Deleting existing objects [{}]".format(object._meta.db_table)
                )
                object.objects.all().delete()

                # get field names
                fields = list(f.name for f in object._meta.fields if f.name != "id")

                if object.__name__ == "CharityClassification":
                    values = tuple(self.charity_classification)
                    fields = ["charity_id", "classification_type", "classification"]
                else:
                    values = tuple(
                        tuple(c.get(f) for f in fields) for c in self.charities
                    )

                # validate the first 10 charities
                for c in values[:20]:
                    d = dict(zip(fields, c))
                    o = object(**d)
                    try:
                        o.full_clean(exclude=["website", "email"])
                    except Exception as e:
                        self.logger("Validation error: {}".format(e), error=True)
                        raise

                # insert new charities
                statement = """INSERT INTO "{table}" ("{fields}") VALUES {placeholder};""".format(
                    table=object._meta.db_table,
                    fields='", "'.join(fields),
                    placeholder="(" + ", ".join(["%s" for f in fields]) + ")"
                    if connection.vendor == "sqlite"
                    else "%s",
                )

                self._execute_many(cursor, statement, values)
                self.logger("Finished table insert [{}]".format(object._meta.db_table))
