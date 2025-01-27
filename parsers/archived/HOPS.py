#!/usr/bin/env python3
# Archived reason: Replaced by the ENTSOE parser for higher data granularity.

"""Parser for power production in Croatia"""


from datetime import datetime
from logging import Logger, getLogger
from typing import Optional, Union

import arrow
import pandas as pd
from requests import Session
from parsers.func import get_data
from parsers.example import paeras_example

Reader = get_data()

class extract_data(paeras_example):
    def __init__(self):
        self.URL = "https://www.hops.hr/Home/PowerExchange"
    def fetch_solar_production(self,
        feed_date, session: Optional[Session] = None, logger: Logger = getLogger(__name__)
    ) -> Union[float, None]:
        """
        Calls extra resource at https://files.hrote.hr/files/EKO_BG/FORECAST/SOLAR/FTP/TEST_DRIVE/<dd.m.yyyy>.json
        to get Solar power production in MW.
        :param feed_date: date_time string from the original HOPS feed
        """


        dt = datetime.strptime(feed_date, "%Y-%m-%d %H:%M:%S")
        # Get all available files
        dates_url = (
            "https://files.hrote.hr/files/EKO_BG/FORECAST/SOLAR/FTP/TEST_DRIVE/dates.json"
        )
        dates = Reader.get_data(session,dates_url,"json")

        # Use latest file to get more up to date estimation
        solar_url = (
            "https://files.hrote.hr/files/EKO_BG/FORECAST/SOLAR/FTP/TEST_DRIVE/{0}".format(
                dates[-1]["Filename"]
            )
        )
        obj = Reader.get_data(session, solar_url, "json")

        df = pd.DataFrame.from_dict(obj["FullPower"]).set_index("Timestamp")
        df.index = pd.to_datetime(df.index)  # cast strings to datetimes
        solar_production_dt = pd.Timestamp(feed_date, tz="Europe/Zagreb").floor("1h")
        try:
            solar = df["Value"].loc[solar_production_dt]
            # Converting to MW
            solar *= 0.001
        except KeyError:
            logger.warning(
                "No value for Solar power production on {0}".format(solar_production_dt)
            )
            solar = None

        return solar


    def fetch_production(self,
        zone_key: str = "HR",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):


        response = Reader.get_data_warn(session=session,url=self.URL, Format="json",target_datetime=target_datetime)
        obj = response

        # We assume the timezone of the time returned is local time and convert to UTC
        date_time = arrow.get(obj["updateTime"]).replace(tzinfo="Europe/Belgrade").to("utc")

        # The json returned contains a list of values
        # 0 - 9 are individual exchanges with neighbouring countries
        # 10 - 13 are cumulative exchanges with neigbouring countries
        # 14 grid frequency
        # 15 total load of Croatia
        # 16 'Ukupna proizvodnja'  total generation
        # 17 'Proizvodnja VE'      wind generation
        df = pd.DataFrame.from_dict(obj["resources"]).set_index("sourceName")

        # Get the wind power generation
        # In some cases value may be negative. It looks like an issue at data source side.
        wind = abs(df["value"].loc["Proizvodnja VE"])

        solar = self.fetch_solar_production(obj["updateTime"], session)

        # Get the total power generation and substract wind and solar power
        unknown = (df["value"].loc["Ukupna proizvodnja"]) - wind
        if solar:
            unknown -= solar

        return [
            {
                "zoneKey": zone_key,
                "datetime": date_time.datetime,
                "production": {"wind": wind, "solar": solar, "unknown": unknown},
                "source": "hops.hr",
            }
        ]


if __name__ == "__main__":
    print("fetch_production(HR)->")
    s = extract_data()
    print(s.fetch_production("HR"))
