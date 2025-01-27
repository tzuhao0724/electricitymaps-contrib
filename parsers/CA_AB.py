#!/usr/bin/env python3

"""Parse the Alberta Electric System Operator's (AESO's) Energy Trading System
(ETS) website.
"""

# Standard library imports
import csv
import logging
import re
import urllib.parse
from datetime import datetime
from logging import Logger, getLogger
from typing import Any, Dict, Optional

# Third-party library imports
import arrow
from requests import Session

# Local library imports
from parsers.lib import validation
from parsers.func import get_data
from parsers.example import paeras_example
class get_data_CA_AB(get_data):
    def get_data_warn(self,session=None,url:str=" ",Format:str = None,target_datetime=None):
        if target_datetime is not None:
            raise NotImplementedError("This parser is not yet able to parse past dates")
        r= session or Session()
        r = r.get(url, params={"contentType": "csv"})
        return r
reader = get_data_CA_AB()
DEFAULT_ZONE_KEY = "CA-AB"
MINIMUM_PRODUCTION_THRESHOLD = 10  # MW
TIMEZONE = "Canada/Mountain"
URL = urllib.parse.urlsplit("http://ets.aeso.ca/ets_web/ip/Market/Reports")
URL_STRING = urllib.parse.urlunsplit(URL)

class extract_data(paeras_example):
    def fetch_exchange(self,
        zone_key1: str = DEFAULT_ZONE_KEY,
        zone_key2: str = "CA-BC",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Request the last known power exchange (in MW) between two countries."""

        response = reader.get_data_warn(session,f"{URL_STRING}/CSDReportServlet",target_datetime=target_datetime)
        interchange = dict(csv.reader(response.text.split("\r\n\r\n")[4].splitlines()))
        flows = {
            f"{DEFAULT_ZONE_KEY}->CA-BC": interchange["British Columbia"],
            f"{DEFAULT_ZONE_KEY}->CA-SK": interchange["Saskatchewan"],
            f"{DEFAULT_ZONE_KEY}->US-MT": interchange["Montana"],
            f"{DEFAULT_ZONE_KEY}->US-NW-NWMT": interchange["Montana"],
        }
        sorted_zone_keys = "->".join(sorted((zone_key1, zone_key2)))
        if sorted_zone_keys not in flows:
            raise NotImplementedError(f"Pair '{sorted_zone_keys}' not implemented")
        return {
            "datetime": self.get_csd_report_timestamp(response.text),
            "sortedZoneKeys": sorted_zone_keys,
            "netFlow": float(flows[sorted_zone_keys]),
            "source": URL.netloc,
        }


    def fetch_price(self,
        zone_key: str = DEFAULT_ZONE_KEY,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> list:
        """Request the last known power price of a given country."""
        response = reader.get_data_warn(session, f"{URL_STRING}/SMPriceReportServlet",target_datetime=target_datetime)
        return [
            {
                "currency": "CAD",
                "datetime": arrow.get(row[0], "MM/DD/YYYY HH", tzinfo=TIMEZONE).datetime,
                "price": float(row[1]),
                "source": URL.netloc,
                "zoneKey": zone_key,
            }
            for row in csv.reader(response.text.split("\r\n\r\n")[2].splitlines()[1:])
            if row[1] != "-"
        ]


    def fetch_production(self,
        zone_key: str = DEFAULT_ZONE_KEY,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> Dict[str, Any]:
        """Request the last known production mix (in MW) of a given country."""
        response = reader.get_data_warn(session, f"{URL_STRING}/CSDReportServlet",target_datetime=target_datetime)
        generation = {
            row[0]: {
                "MC": float(row[1]),  # maximum capability
                "TNG": float(row[2]),  # total net generation
            }
            for row in csv.reader(response.text.split("\r\n\r\n")[3].splitlines())
        }
        return validation.validate(
            {
                "capacity": {
                    "gas": generation["GAS"]["MC"],
                    "hydro": generation["HYDRO"]["MC"],
                    "battery storage": generation["ENERGY STORAGE"]["MC"],
                    "solar": generation["SOLAR"]["MC"],
                    "wind": generation["WIND"]["MC"],
                    "biomass": generation["OTHER"]["MC"],
                    "unknown": generation["DUAL FUEL"]["MC"],
                    "coal": generation["COAL"]["MC"],
                },
                "datetime": self.get_csd_report_timestamp(response.text),
                "production": {
                    "gas": generation["GAS"]["TNG"],
                    "hydro": generation["HYDRO"]["TNG"],
                    "solar": generation["SOLAR"]["TNG"],
                    "wind": generation["WIND"]["TNG"],
                    "biomass": generation["OTHER"]["TNG"],
                    "unknown": generation["DUAL FUEL"]["TNG"],
                    "coal": generation["COAL"]["TNG"],
                },
                "source": URL.netloc,
                "storage": {
                    "battery": generation["ENERGY STORAGE"]["TNG"],
                },
                "zoneKey": zone_key,
            },
            logger,
            floor=MINIMUM_PRODUCTION_THRESHOLD,
            remove_negative=True,
        )


    def get_csd_report_timestamp(self,report):
        """Get the timestamp from a current supply/demand (CSD) report."""
        return arrow.get(
            re.search(r'"Last Update : (.*)"', report).group(1),
            "MMM DD, YYYY HH:mm",
            tzinfo=TIMEZONE,
        ).datetime


if __name__ == "__main__":
    # Never used by the electricityMap backend, but handy for testing.
    s = extract_data()
    print("fetch_production() ->")
    print(s.fetch_production())
    print("fetch_price() ->")
    print(s.fetch_price())
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, CA-BC) ->")
    print(s.fetch_exchange(DEFAULT_ZONE_KEY, "CA-BC"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, CA-SK) ->")
    print(s.fetch_exchange(DEFAULT_ZONE_KEY, "CA-SK"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, US-MT) ->")
    print(s.fetch_exchange(DEFAULT_ZONE_KEY, "US-MT"))
    print(f"fetch_exchange({DEFAULT_ZONE_KEY}, US-NW-NWMT) ->")
    print(s.fetch_exchange(DEFAULT_ZONE_KEY, "US-NW-NWMT"))
