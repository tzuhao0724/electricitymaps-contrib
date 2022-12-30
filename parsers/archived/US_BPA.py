#!/usr/bin/env python3
# Archive reason: No longer in use.

"""Parser for the Bonneville Power Administration area of the USA."""


import logging
from io import StringIO

import arrow
import pandas as pd
import requests
from parsers.func import get_data
from parsers.example import paeras_example
GENERATION_URL = "https://transmission.bpa.gov/business/operations/Wind/baltwg.txt"

GENERATION_MAPPING = {
    "Wind": "wind",
    "Hydro": "hydro",
    "Fossil/Biomass": "unknown",
    "Nuclear": "nuclear",
}
reader = get_data()


class extract_data(paeras_example):
    def timestamp_converter(self,):
        """Turns a timestamp str into an aware datetime object."""

        arr_dt_naive = arrow.get(timestamp, "MM/DD/YYYY HH:mm")
        dt_aware = arr_dt_naive.replace(tzinfo="America/Los_Angeles").datetime

        return dt_aware


    def data_processor(self,df, logger) -> list:
        """
        Takes a dataframe and drops all generation rows that are empty or more than 1 day old.
        Turns each row into a dictionary and removes any generation types that are unknown.

        :return: list of tuples in the form of (datetime, production).
        """

        df = df.dropna(thresh=2)
        df.columns = df.columns.str.strip()

        # 5min data for the last 24 hours.
        df = df.tail(288)
        df["Date/Time"] = df["Date/Time"].map(self.timestamp_converter)

        known_keys = GENERATION_MAPPING.keys() | {"Date/Time", "Load"}
        column_headers = set(df.columns)

        unknown_keys = column_headers - known_keys

        for k in unknown_keys:
            logger.warning(
                "New data {} seen in US-BPA data source".format(k), extra={"key": "US-BPA"}
            )

        keys_to_remove = unknown_keys | {"Load"}

        processed_data = []
        for index, row in df.iterrows():
            production = row.to_dict()

            dt = production.pop("Date/Time")
            dt = dt.to_pydatetime()
            mapped_production = {
                GENERATION_MAPPING[k]: v
                for k, v in production.items()
                if k not in keys_to_remove
            }

            processed_data.append((dt, mapped_production))

        return processed_data


    def fetch_production(self,
        zone_key="US-BPA",
        session=None,
        target_datetime=None,
        logger=logging.getLogger(__name__),
    ) -> list:
        """Requests the last known production mix (in MW) of a given zone."""

        req = reader.get_data_warn(session,GENERATION_URL,target_datetime=target_datetime)
        raw_data = pd.read_table(StringIO(req.text), skiprows=11)
        processed_data = self.data_processor(raw_data, logger)

        data = []
        for item in processed_data:
            datapoint = {
                "zoneKey": zone_key,
                "datetime": item[0],
                "production": item[1],
                "storage": {},
                "source": "bpa.gov",
            }

            data.append(datapoint)

        return data


if __name__ == "__main__":
    s = extract_data()
    print("fetch_production() ->")

    print(s.fetch_production())
