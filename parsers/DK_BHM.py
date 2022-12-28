#!/usr/bin/env python3
# The arrow library is used to handle datetimes
from datetime import datetime
from logging import Logger, getLogger
from typing import Optional

import arrow
from requests import Session
from parsers.func import get_data
reader = get_data()

PRODUCTION_MAPPING = {
    "wind": "wind_turbines",
    "biomass": "factory",
    "solar": "solar_cells",
}


def _fetch_data(session: Optional[Session] = None):

    url = "http://bornholm.powerlab.dk/visualizer/latestdata"
    obj = reader.get_data(session,url,"json")
    return obj


def fetch_production(
    zone_key: str = "DK-BHM",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known production mix (in MW) of a given country."""
    if target_datetime:
        raise NotImplementedError("This parser is not yet able to parse past dates")

    obj = _fetch_data(session)

    data = {
        "zoneKey": zone_key,
        "production": {},
        "storage": {},
        "source": "bornholm.powerlab.dk",
        "datetime": arrow.get(obj["latest"]).datetime,
    }
    for productionKey, objKey in PRODUCTION_MAPPING.items():
        data["production"][productionKey] = obj["sub"][objKey]

    return data


def fetch_exchange(
    zone_key1: str = "DK-BHM",
    zone_key2: str = "SE-SE4",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known power exchange (in MW) between two countries."""

    obj = _fetch_data(session)

    data = {
        "sortedZoneKeys": "->".join(sorted([zone_key1, zone_key2])),
        "source": "bornholm.powerlab.dk",
        "datetime": arrow.get(obj["latest"]).datetime,
    }

    # Country codes are sorted in order to enable easier indexing in the database
    sorted_zone_keys = sorted([zone_key1, zone_key2])
    # Here we assume that the net flow returned by the api is the flow from
    # country1 to country2. A positive flow indicates an export from country1
    # to country2. A negative flow indicates an import.
    netFlow = obj["sub"]["seacable"]  # Export is positive
    # The net flow to be reported should be from the first country to the second
    # (sorted alphabetically). This is NOT necessarily the same direction as the flow
    # from country1 to country2
    data["netFlow"] = netFlow if zone_key1 == sorted_zone_keys[0] else -1 * netFlow

    return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print("fetch_production() ->")
    print(fetch_production())
    print("fetch_exchange(DK-BHM, SE-SE4) ->")
    print(fetch_exchange("DK-BHM", "SE-SE4"))
