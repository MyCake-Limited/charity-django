# Generated by Django 4.1.5 on 2023-01-18 14:52

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ccew", "0005_alter_merger_transferee_regno_and_more"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="merger",
            options={
                "verbose_name": "Charity Merger",
                "verbose_name_plural": "Register of Mergers",
            },
        ),
    ]