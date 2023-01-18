# Generated by Django 4.1.5 on 2023-01-13 16:37

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Charity",
            fields=[
                (
                    "reg_charity_number",
                    models.IntegerField(
                        primary_key=True,
                        serialize=False,
                        verbose_name="Reg charity number",
                    ),
                ),
                (
                    "sub_charity_number",
                    models.IntegerField(default=0, verbose_name="Sub charity number"),
                ),
                (
                    "charity_name",
                    models.CharField(
                        db_index=True, max_length=255, verbose_name="Charity name"
                    ),
                ),
                ("date_registered", models.DateField(verbose_name="Date registered")),
                ("status", models.CharField(max_length=255, verbose_name="Status")),
                (
                    "date_for_financial_year_ending",
                    models.DateField(verbose_name="Date for financial year ending"),
                ),
                ("total_income", models.BigIntegerField(verbose_name="Total income")),
                (
                    "total_spending",
                    models.BigIntegerField(verbose_name="Total spending"),
                ),
                (
                    "charitable_spending",
                    models.BigIntegerField(verbose_name="Charitable spending"),
                ),
                (
                    "income_generation_and_governance",
                    models.BigIntegerField(
                        verbose_name="Income generation and governance"
                    ),
                ),
                (
                    "retained_for_future_use",
                    models.BigIntegerField(verbose_name="Retained for future use"),
                ),
                (
                    "public_address",
                    models.TextField(
                        blank=True, null=True, verbose_name="Public address"
                    ),
                ),
                (
                    "website",
                    models.URLField(blank=True, null=True, verbose_name="Website"),
                ),
                (
                    "email",
                    models.EmailField(
                        blank=True, max_length=254, null=True, verbose_name="Email"
                    ),
                ),
                (
                    "telephone",
                    models.CharField(
                        blank=True, max_length=255, null=True, verbose_name="Telephone"
                    ),
                ),
                (
                    "company_number",
                    models.CharField(
                        blank=True,
                        max_length=255,
                        null=True,
                        verbose_name="Company number",
                    ),
                ),
            ],
            options={
                "verbose_name": "Charity",
                "verbose_name_plural": "Charities",
            },
        ),
        migrations.CreateModel(
            name="CharityClassification",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "classification_type",
                    models.CharField(
                        choices=[
                            ("What the charity does", "What The Charity Does"),
                            ("Who the charity helps", "Who The Charity Helps"),
                            ("How the charity works", "How The Charity Works"),
                        ],
                        max_length=255,
                        verbose_name="Classification type",
                    ),
                ),
                (
                    "classification",
                    models.CharField(max_length=255, verbose_name="Classification"),
                ),
                (
                    "charity",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="classifications",
                        to="ccni.charity",
                        verbose_name="Charity",
                    ),
                ),
            ],
            options={
                "verbose_name": "Charity classification",
                "verbose_name_plural": "Charity classifications",
                "unique_together": {
                    ("charity_id", "classification_type", "classification")
                },
            },
        ),
    ]
