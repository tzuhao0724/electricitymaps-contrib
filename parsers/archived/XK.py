#!/usr/bin/env python3
# Archive reason: No longer in use.

import datetime
import logging
import re

# The arrow library is used to handle datetimes
import arrow

# The request library is used to fetch content through HTTP
import requests

# Tablib is used to parse XLSX files
import tablib
from parsers.func import get_data
reader = get_data()
from parsers.example import paeras_example
# please try to write PEP8 compliant code (use a linter). One of PEP8's
# requirement is to limit your line length to 79 characters.

class extract_data(paeras_example):
    def fetch_production(self,
        zone_key="XK",
        session=None,
        target_datetime: datetime.datetime = None,
        logger: logging.Logger = logging.getLogger(__name__),
    ) -> list:
        """Requests the last known production mix (in MW) of a given country."""

        url = "https://www.kostt.com/Content/ViewFiles/Transparency/BasicMarketDataOnGeneration/Prodhimi%20aktual%20gjenerimi%20faktik%20i%20energjise%20elektrike.xlsx"


        res = reader.get_data_warn(session=session,url= url,target_datetime=target_datetime)
        assert res.status_code == 200, "XK (Kosovo) parser: GET {} returned {}".format(
            url, res.status_code
        )

        sheet = tablib.Dataset().load(res.content, headers=False)

        productions = {}  # by time
        for i in range(5, 1000):
            try:
                row = sheet[i]
            except IndexError:
                break
            time = row[1]
            if time is None:
                break
            if isinstance(time, float):
                time = datetime.time(hour=round(time * 24) % 24)
            time_str = time.strftime("%H:%M")
            assert "TC KOSOVA" in row[3], "Parser assumes only coal data"
            prod = float(row[2])
            productions[time_str] = productions.get(time_str, 0.0) + prod

        date_match = re.search(
            r"ACTUAL\s+GENERATION\s+FOR\s+DAY\s+(\d+)\.(\d+)\.(\d+)", sheet[1][1]
        )
        assert date_match is not None, "Date not found in spreadsheet"
        date_str = (
            date_match.group(3)
            + "-"
            + date_match.group(2)
            + "-"
            + date_match.group(1)
            + " "
        )

        data = []
        for time_str, prod in productions.items():
            timestamp = arrow.get(date_str + time_str).replace(tzinfo="Europe/Belgrade")
            timestamp = timestamp.shift(hours=-1)  # shift to start of period
            if time_str == "00:00":
                # Based on the apparent discontinuity in production and the order in the spreadsheet
                # it seems that the last data-point belongs to the next day
                timestamp = timestamp.shift(days=1)
            data.append(
                {
                    "zoneKey": zone_key,
                    "production": {"coal": prod},
                    "storage": {},
                    "source": "kostt.com",
                    "datetime": timestamp.datetime,
                }
            )

        return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    s = extract_data()
    print("fetch_production() ->")
    for datum in s.fetch_production():
        print(datum)
