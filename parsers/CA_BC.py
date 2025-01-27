#!/usr/bin/env python3

# The arrow library is used to handle datetimes
from datetime import datetime
from logging import Logger, getLogger
from typing import Optional
from parsers.func import get_data
import arrow
from requests import Session
from parsers.example import paeras_example
reader = get_data()
# More info:
# https://www.bchydro.com/energy-in-bc/our_system/transmission/transmission-system/actual-flow-data.html

timezone = "Canada/Pacific"

class extract_data(paeras_example):
    def fetch_exchange(self,
        zone_key1: str,
        zone_key2: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Requests the last known power exchange (in MW) between two countries."""
        url = "https://www.bchydro.com/bctc/system_cms/actual_flow/latest_values.txt"
        response = reader.get_data_warn(session,url,target_datetime=target_datetime)
        obj = response.text.split("\r\n")[1].replace("\r", "").split(",")

        datetime = arrow.get(
            arrow.get(obj[0], "DD-MMM-YY HH:mm:ss").datetime, timezone
        ).datetime

        sortedZoneKeys = "->".join(sorted([zone_key1, zone_key2]))

        if sortedZoneKeys == "CA-BC->US-BPA" or sortedZoneKeys == "CA-BC->US-NW-BPAT":
            netFlow = float(obj[1])
        elif sortedZoneKeys == "CA-AB->CA-BC":
            netFlow = -1 * float(obj[2])
        else:
            raise NotImplementedError("This exchange pair is not implemented")

        return {
            "datetime": datetime,
            "sortedZoneKeys": sortedZoneKeys,
            "netFlow": netFlow,
            "source": "bchydro.com",
        }


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    s  = extract_data()
    print("fetch_exchange(CA-BC, US-BPA) ->")
    print(s.fetch_exchange("CA-BC", "US-BPA"))
    print("fetch_exchange(CA-AB, CA-BC) ->")
    print(s.fetch_exchange("CA-AB", "CA-BC"))
