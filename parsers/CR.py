#!/usr/bin/env python3
# coding=utf-8


from datetime import datetime, time
from logging import Logger, getLogger
from typing import Optional

import arrow
import pandas as pd
from bs4 import BeautifulSoup
from requests import Session
from parsers.func import get_data
from CR_data import *
from parsers.example import paeras_example
reader = get_data()



class extract_data(paeras_example):
    def empty_record(self,zone_key: str):
        return {
            "zoneKey": zone_key,
            "capacity": {},
            "production": {
                "biomass": 0.0,
                "coal": 0.0,
                "gas": 0.0,
                "hydro": 0.0,
                "nuclear": 0.0,
                "oil": 0.0,
                "solar": 0.0,
                "wind": 0.0,
                "geothermal": 0.0,
                "unknown": 0.0,
            },
            "storage": {},
            "source": "grupoice.com",
        }


    def df_to_data(self,zone_key: str, day, df, logger: Logger):
        df = df.dropna(axis=1, how="any")
        # Check for empty dataframe
        if df.shape == (1, 1):
            return []
        df = df.drop(["Intercambio Sur", "Intercambio Norte", "Total"], errors="ignore")
        df = df.iloc[:, :-1]

        results = []
        unknown_plants = set()
        hour = 0
        for column in df:
            data = self.empty_record(zone_key)
            data_time = day.replace(hour=hour, minute=0, second=0, microsecond=0).datetime
            for index, value in df[column].items():
                source = POWER_PLANTS.get(index)
                if not source:
                    source = "unknown"
                    unknown_plants.add(index)
                data["datetime"] = data_time
                data["production"][source] += max(0.0, value)
            hour += 1
            results.append(data)

        for plant in unknown_plants:
            logger.warning(
                "{} is not mapped to generation type".format(plant), extra={"key": zone_key}
            )

        return results


    def fetch_production(self,
        zone_key: str = "CR",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        # ensure we have an arrow object.
        # if no target_datetime is specified, this defaults to now.
        target_datetime = arrow.get(target_datetime).to(TIMEZONE)

        # if before 01:30am on the current day then fetch previous day due to
        # data lag.
        today = arrow.get().to(TIMEZONE).date()
        if target_datetime.date() == today:
            target_datetime = (
                target_datetime
                if target_datetime.time() >= time(1, 30)
                else target_datetime.shift(days=-1)
            )

        if target_datetime < arrow.get("2012-07-01"):
            # data availability limit found by manual trial and error
            logger.error(
                "CR API does not provide data before 2012-07-01, "
                "{} was requested".format(target_datetime),
                extra={"key": zone_key},
            )
            return None

        # Do not use existing session as some amount of cache is taking place
        url = "https://apps.grupoice.com/CenceWeb/CencePosdespachoNacional.jsf"
        response = reader.get_data(session,url)

        soup = BeautifulSoup(response.text, "html.parser")
        jsf_view_state = soup.find("input", {"name": "javax.faces.ViewState"})["value"]

        data = [
            ("formPosdespacho:txtFechaInicio_input", target_datetime.format(DATE_FORMAT)),
            ("formPosdespacho:pickFecha", ""),
            ("formPosdespacho_SUBMIT", 1),
            ("javax.faces.ViewState", jsf_view_state),
        ]
        r = session or Session()
        response = r.post(url, data=data)

        # tell pandas which table to use by providing CHARACTERISTIC_NAME
        df = pd.read_html(
            response.text, match=CHARACTERISTIC_NAME, skiprows=1, index_col=0
        )[0]

        results = self.df_to_data(zone_key, target_datetime, df, logger)

        return results


    def fetch_exchange(self,
        zone_key1: str = "CR",
        zone_key2: str = "NI",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Requests the last known power exchange (in MW) between two regions."""
        if target_datetime:
            raise NotImplementedError("This parser is not yet able to parse past dates")

        sorted_zone_keys = "->".join(sorted([zone_key1, zone_key2]))

        df = pd.read_csv(
            "http://www.enteoperador.org/newsite/flash/data.csv", index_col=False
        )

        if sorted_zone_keys == "CR->NI":
            flow = df["NICR"][0]
        elif sorted_zone_keys == "CR->PA":
            flow = -1 * df["CRPA"][0]
        else:
            raise NotImplementedError("This exchange pair is not implemented")

        data = {
            "datetime": arrow.now(TIMEZONE).datetime,
            "sortedZoneKeys": sorted_zone_keys,
            "netFlow": flow,
            "source": "enteoperador.org",
        }

        return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    from pprint import pprint
    s = extract_data()
    print("fetch_production() ->")
    pprint(s.fetch_production())

    print('fetch_production(target_datetime=arrow.get("2018-03-13T12:00Z") ->')
    pprint(s.fetch_production(target_datetime=arrow.get("2018-03-13T12:00Z")))

    # this should work
    print('fetch_production(target_datetime=arrow.get("2013-03-13T12:00Z") ->')
    pprint(s.fetch_production(target_datetime=arrow.get("2013-03-13T12:00Z")))

    # this should return None
    print('fetch_production(target_datetime=arrow.get("2007-03-13T12:00Z") ->')
    pprint(s.fetch_production(target_datetime=arrow.get("2007-03-13T12:00Z")))

    print("fetch_exchange() ->")
    print(s.fetch_exchange())
