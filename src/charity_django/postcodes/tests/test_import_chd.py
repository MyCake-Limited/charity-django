import os

import requests_mock
from django.test import TestCase
from requests import Session

from charity_django.postcodes.management.commands.import_chd import (
    CHD_URL,
    Command,
)
from charity_django.postcodes.models import GeoCode


class TestImportCHD(TestCase):
    def mock_csv_downloads(self, m):
        dirname = os.path.dirname(__file__)
        with open(
            os.path.join(dirname, "data", "Code_History_Database_May_2023_UK.zip"),
            "rb",
        ) as a:
            m.get(CHD_URL, content=a.read())

    def test_set_session(self):
        command = Command()

        assert hasattr(command, "session") is False
        command.set_session()
        assert hasattr(command, "session") is True
        assert isinstance(command.session, Session)

    def test_handle(self):
        command = Command()

        with requests_mock.Mocker() as m:
            self.mock_csv_downloads(m)
            command.handle(debug=False, cache=False)
            assert GeoCode.objects.count() == 500
            assert GeoCode.objects.filter(STATUS="live").count() == 417
            assert (
                GeoCode.objects.get(GEOGCD="E04005721").GEOGNM
                == "Skidbrooke with Saltfleet Haven"
            )
            assert GeoCode.objects.get(GEOGCD="E33003018").GEOGNM is None
