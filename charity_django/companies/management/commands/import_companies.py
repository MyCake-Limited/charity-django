import argparse
import csv
import datetime
import io
import re
import zipfile
from collections import defaultdict

import requests
import requests_cache
import tqdm
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from requests_html import HTMLSession

from charity_django.companies.ch_api import (
    ACCOUNTS_TYPE_LOOKUP,
    COMPANY_CATEGORY_LOOKUP,
    COMPANY_STATUS_LOOKUP,
    AccountTypes,
    CompanyStatuses,
    CompanyTypes,
)
from charity_django.companies.models import (
    Company,
    CompanySICCode,
    PreviousName,
    SICCode,
)
from charity_django.utils.cachedsession import CachedHTMLSession

from ._company_sql import UPDATE_COMPANIES

DEFAULT_DATE_FORMAT = "%Y-%m-%d"


MODEL_UPDATES = {
    Company: {
        "update_conflicts": True,
        "update_fields": [
            f.get_attname_column()[0]
            for f in Company._meta.get_fields()
            if f.name not in ["CompanyNumber"] and hasattr(f, "get_attname_column")
        ],
        "unique_fields": ["CompanyNumber"],
    },
    PreviousName: {
        "update_conflicts": True,
        "update_fields": [
            "in_latest_update",
        ],
        "unique_fields": ["company", "CompanyName"],
    },
    CompanySICCode: {
        "update_conflicts": True,
        "update_fields": [
            "in_latest_update",
        ],
        "unique_fields": ["company", "sic_code"],
    },
}


class Command(BaseCommand):
    name = "companies"
    start_url = "http://download.companieshouse.gov.uk/en_output.html"
    zip_regex = re.compile(r".*/BasicCompanyData-.*\.zip")
    id_field = "CompanyNumber"
    date_fields = [
        "DissolutionDate",
        "IncorporationDate",
        "Accounts_NextDueDate",
        "Accounts_LastMadeUpDate",
        "Returns_NextDueDate",
        "Returns_LastMadeUpDate",
        "ConfStmtNextDueDate",
        "ConfStmtLastMadeUpDate",
    ]
    date_format = "%d/%m/%Y"
    bulk_limit = 10000
    source = {
        "title": "Free Company Data Product",
        "description": "The Free Company Data Product is a downloadable data snapshot containing \
            basic company data of live companies on the register. This snapshot is provided as \
            ZIP files containing data in CSV format and is split into multiple files for ease of \
            downloading.",
        "identifier": "companies",
        "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
        "license_name": "Open Government Licence v3.0",
        "issued": "",
        "modified": "",
        "publisher": {
            "name": "Companies House",
            "website": "https://www.gov.uk/government/organisations/companies-house",
        },
        "distribution": [],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = defaultdict(dict)
        self.object_count = defaultdict(lambda: 0)
        self.now = datetime.datetime.now()
        self.sic_code_cache = {}

    def add_arguments(self, parser):
        parser.add_argument(
            "--cache",
            action=argparse.BooleanOptionalAction,
            help="Cache request",
            default=settings.DEBUG,
        )
        parser.add_argument(
            "--debug",
            action=argparse.BooleanOptionalAction,
            help="Debug",
            default=settings.DEBUG,
        )

    def handle(self, *args, **options):
        self.debug = options["debug"]
        with transaction.atomic(), connection.cursor() as cursor:

            Company.objects.update(in_latest_update=False)
            CompanySICCode.objects.update(in_latest_update=False)
            PreviousName.objects.update(in_latest_update=False)

            self.set_session(install_cache=options["cache"])
            self.fetch_file()
            for source_url, response in self.files.items():
                self.parse_file(response, source_url)

            for model in [CompanySICCode, PreviousName]:
                print("To keep: ", model.objects.filter(in_latest_update=True).count())
                print(
                    "To delete: ", model.objects.exclude(in_latest_update=True).count()
                )
                deleted, _ = model.objects.exclude(in_latest_update=True).delete()
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Deleted {deleted} {model.__name__} records not in latest update"
                    )
                )

            for title, sql in UPDATE_COMPANIES.items():
                cursor.execute(sql)
                self.stdout.write(self.style.SUCCESS(f"Executed {title}"))

    def set_session(self, install_cache=False):
        if install_cache:
            self.stdout.write("Using requests_cache")
            self.session = CachedHTMLSession(
                cache_name="companies_house_download_cache",
                cache_control=False,
                expire_after=datetime.timedelta(days=10),
            )
        else:
            self.session = HTMLSession()

    def fetch_file(self):
        self.files = {}
        response = self.session.get(self.start_url)
        response.raise_for_status()
        for link in response.html.absolute_links:
            if self.zip_regex.match(link):
                self.stdout.write("Fetching: {}".format(link))
                try:
                    self.files[link] = self.session.get(link)
                    if getattr(self.files[link], "from_cache", False):
                        self.stdout.write("From cache")
                    else:
                        self.stdout.write("From network")
                    self.files[link].raise_for_status()
                except requests.exceptions.ChunkedEncodingError as err:
                    self.stdout.write(
                        self.style.ERROR("Error fetching: {}".format(link))
                    )
                    self.stdout.write(self.style.ERROR(str(err)))

    def parse_file(self, response, source_url):
        self.stdout.write("Opening: {}".format(source_url))
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for f in z.infolist():
                self.stdout.write("Opening: {}".format(f.filename))
                with z.open(f) as csvfile:
                    reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf8"))
                    for index, row in tqdm.tqdm(enumerate(reader)):
                        self.parse_row(row)
                        if self.debug and index >= 100:
                            break
                    self.save_all_records()
        response = None

    def parse_row(self, row):
        row = {k.strip().replace(".", "_"): row[k] for k in row}
        row = self.clean_fields(row)
        row = self.clean_categories(row)

        previous_names = {}
        sic_codes = []
        record = {}
        for k in row:
            if k.startswith("PreviousName_"):
                pn = k.split("_")
                if row[k] and row[k] != "":
                    if pn[1] not in previous_names:
                        previous_names[pn[1]] = {}

                    if pn[2] == "CONDATE":
                        previous_names[pn[1]][pn[2]] = datetime.datetime.strptime(
                            row[k], "%d/%m/%Y"
                        ).date()
                        previous_names[pn[1]]["nameno"] = pn[1]
                    else:
                        previous_names[pn[1]][pn[2]] = row[k]

            elif k.startswith("SICCode_"):
                if row[k] and row[k].replace("None Supplied", "") != "":
                    sic_code, sic_title = (
                        v.strip() for v in row[k].split(" - ", maxsplit=1)
                    )
                    if sic_code not in self.sic_code_cache:
                        new_code, _ = SICCode.objects.update_or_create(
                            code=sic_code, defaults={"title": sic_title}
                        )
                        self.sic_code_cache[sic_code] = new_code
                    sic_codes.append(self.sic_code_cache[sic_code])
            else:
                record[k] = row[k]

        record["previous_names"] = list(previous_names.values())
        record["sic_codes"] = sic_codes

        address1 = []
        for f in [
            "RegAddress_CareOf",
            "RegAddress_POBox",
            "RegAddress_AddressLine1",
            "RegAddress_AddressLine2",
        ]:
            if record.get(f):
                address1.append(record.get(f))

        company = Company(
            **{
                k: v
                for k, v in record.items()
                if k not in ("sic_codes", "previous_names")
            },
            last_updated=self.now,
            in_latest_update=True,
        )

        self.add_record(Company, company)
        for n in record["previous_names"]:
            self.add_record(
                PreviousName,
                {
                    "company": company,
                    "CompanyName": n["CompanyName"],
                    "ConDate": n["CONDATE"],
                    "in_latest_update": True,
                },
            )
        for s in record["sic_codes"]:
            self.add_record(
                CompanySICCode,
                {
                    "company": company,
                    "sic_code": s,
                    "in_latest_update": True,
                },
            )

    def clean_fields(self, record):
        for f in record.keys():
            # clean blank values
            if record[f] == "":
                record[f] = None

            # clean date fields
            elif f in getattr(self, "date_fields", []) and isinstance(record[f], str):
                date_format = self.date_format
                if isinstance(date_format, dict):
                    date_format = date_format.get(f, DEFAULT_DATE_FORMAT)

                try:
                    if record.get(f):
                        record[f] = datetime.datetime.strptime(
                            record.get(f).strip(), date_format
                        )
                except ValueError:
                    record[f] = None

            # clean boolean fields
            elif f in getattr(self, "bool_fields", []):
                if isinstance(record[f], str):
                    val = record[f].lower().strip()
                    if val in ["f", "false", "no", "0", "n"]:
                        record[f] = False
                    elif val in ["t", "true", "yes", "1", "y"]:
                        record[f] = True

            # strip string fields
            elif isinstance(record[f], str):
                record[f] = record[f].strip().replace("\x00", "")
        return record

    def clean_categories(self, row):
        cats = [
            ("CompanyCategory", COMPANY_CATEGORY_LOOKUP, CompanyTypes),
            ("CompanyStatus", COMPANY_STATUS_LOOKUP, CompanyStatuses),
            ("Accounts_AccountCategory", ACCOUNTS_TYPE_LOOKUP, AccountTypes),
        ]
        for field, lookup, enum in cats:
            row[field] = lookup.get(row.get(field), row.get(field))
            if isinstance(row[field], enum):
                row[field] = row[field].value
        return row

    def add_record(self, model, record):
        if isinstance(record, dict):
            record = model(**record)
        if model._meta.unique_together:
            unique_fields = tuple(
                getattr(record, f) for f in model._meta.unique_together[0]
            )
        else:
            unique_fields = (record.pk,)
        self.records[model][unique_fields] = record
        if len(self.records[Company]) >= self.bulk_limit:
            self.save_all_records()

    def save_records(self, model):
        self.stdout.write(
            "Saving {:,.0f} {} records".format(len(self.records[model]), model.__name__)
        )
        model.objects.bulk_create(
            self.records[model].values(), **MODEL_UPDATES.get(model, {})
        )
        self.object_count[model] += len(self.records[model])
        self.stdout.write(
            "Saved {:,.0f} {} records ({:,.0f} total)".format(
                len(self.records[model]),
                model.__name__,
                self.object_count[model],
            )
        )
        self.records[model] = {}

    def save_all_records(self):
        for model, records in self.records.items():
            if len(records):
                self.save_records(model)
