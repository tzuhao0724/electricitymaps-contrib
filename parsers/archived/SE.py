# Archived reason: No longer in use and data url has changed.

import datetime
from collections import defaultdict

import arrow
import pytz
import requests

from parsers.lib.config import refetch_frequency
from parsers.func import get_data
from parsers.example import paeras_example
reader = get_data()

SVK_URL = (
    "http://www.svk.se/ControlRoom/GetProductionHistory/"
    "?productionDate={date}&countryCode={zoneKey}"
)

# what the value refer to (FYI)
NAMES_MEANING = {
    2: "nuclear",
    4: "thermal",
    5: "wind",
    6: "unknown",
    1: "production",
    7: "consumption",
    3: "hydro",
}
# mapping from the value to electricity map values
MAPPING = {
    2: "nuclear",
    4: "unknown",
    5: "wind",
    6: "unknown",
    1: None,
    7: None,
    3: "hydro",
}

class extract_data(paeras_example):
    @refetch_frequency(datetime.timedelta(days=1))
    def fetch_production(self,
        zone_key="SE", session=None, target_datetime=None, logger=None
    ) -> list:
        # parse target_datetime - and convert None to now
        target_datetime = arrow.get(target_datetime).datetime
        url = SVK_URL.format(date=target_datetime.strftime("%Y-%m-%d"), zoneKey="SE")
        data = reader.get_data(session,url,"json")


        # creating a dict looking like
        # {'2018-01-01 01:00': {'nuclear': xxx, 'hydro': ...}, ...}
        productions = defaultdict(lambda: defaultdict(lambda: 0))
        for sub_data in data:
            # sub_data is a list of (x, y) values for that prod type
            # x is a timestamp, y a value (MWH)
            name = MAPPING[sub_data["name"]]
            if not name:
                continue
            for value in sub_data["data"]:
                dt = datetime.datetime.fromtimestamp(value["x"] / 1000)
                prod = value["y"]
                productions[dt][name] += prod

        datetimes = sorted(list(productions))

        # SVK returns almost a point per minute, but only nuclear is updated
        # with that granularity. Other points are updated every hour, at minute 6
        to_return = []
        last_inserted_datetime = datetime.datetime(2010, 1, 1)
        for dt in datetimes:
            if (dt - last_inserted_datetime).total_seconds() < 3200 or dt.minute < 7:
                continue
            last_inserted_datetime = dt
            prod = productions[dt]
            to_return.append(
                {
                    "production": dict(prod),
                    "datetime": dt.replace(minute=0).replace(tzinfo=pytz.utc),
                    "zoneKey": "SE",
                    "storage": {},
                    "source": "svk.se",
                }
            )

        return to_return


if __name__ == "__main__":
    s = extract_data()
    print(s.fetch_production())
