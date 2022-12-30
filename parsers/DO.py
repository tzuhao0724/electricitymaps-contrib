#!/usr/bin/env python3

from collections import defaultdict
from datetime import datetime
from logging import Logger, getLogger
from math import isnan
from operator import itemgetter
from typing import List, Optional

import arrow
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from requests import Session
from parsers.func import get_data
from parsers.example import paeras_example
from DO_data import *
reader = get_data()

# This parser gets hourly electricity generation data from oc.org.do for the Dominican Republic.
# The data is in MWh but since it is updated hourly we can view it as MW.
# Solar generation now has some data available but multiple projects are planned/under construction.

url = "https://apps.oc.org.do/reportesgraficos/reportepostdespacho.aspx"

class extract_data(paeras_example):
    def get_data_DO(self,session: Optional[Session] = None) -> list:
        """
        Makes a request to source url.
        Finds main table and creates a list of all table elements in string format.
        """

        data = []

        data_req = reader.get_data(session,url)

        soup = BeautifulSoup(data_req.content, "lxml")

        tbs = soup.find("table", id="PostdespachoUnidadesTermicasGrid_DXMainTable")
        rows = tbs.find_all("td")

        for row in rows:
            num = row.getText().strip()
            data.append(str(num))

        return data


    def floater(self,item):
        """
        Attempts to convert any item given to a float.
        Returns item if it fails.
        """

        try:
            return float(item)
        except ValueError:
            return item


    def chunker(self,big_lst) -> dict:
        """
        Breaks a big list into a list of lists.
        Removes any list with no data then turns remaining
        lists into key: value pairs with first element from the list being the key.
        """

        chunks = [big_lst[x : x + 27] for x in range(0, len(big_lst), 27)]

        # Remove the list if it contains no data.
        for chunk in chunks:
            if any(chunk):
                continue
            else:
                chunks.remove(chunk)

        chunked_list = {words[0]: words[1:] for words in chunks}

        return chunked_list


    def data_formatter(self,data) -> dict:
        """
        Takes data and finds relevant sections.
        Formats and breaks data into usable parts.
        """

        find_thermal_index = data.index("GRUPO: T\xe9rmica")
        find_totals_index = data.index("Total T\xe9rmico")
        find_totals_end = data.index("Total Programado")

        ufthermal = data[find_thermal_index + 3 : find_totals_index - 59]
        total_data = data[find_totals_index:find_totals_end]

        # Remove all company names.
        for val in ufthermal:
            if ":" in val:
                i = ufthermal.index(val)
                del ufthermal[i : i + 3]

        formatted_thermal = self.chunker([self.floater(item) for item in ufthermal])
        mapped_totals = [total_mapping.get(x, x) for x in total_data]
        formatted_totals = self.chunker([self.floater(item) for item in mapped_totals])

        return {"totals": formatted_totals, "thermal": formatted_thermal}


    def data_parser(self,formatted_data):
        """
        Converts formatted data into a pandas dataframe.
        Removes any empty rows.
        Returns a DataFrame.
        """

        hours = list(range(1, 24)) + [0] + [25, 26]
        dft = pd.DataFrame(formatted_data, index=hours)

        dft = dft.drop(dft.index[[-1, -2]])
        dft = dft.replace("", np.nan)
        dft = dft.dropna(how="all")

        return dft


    def thermal_production(self,df, logger: Logger) -> List[dict]:
        """
        Takes DataFrame and finds thermal generation for each hour.
        Removes any non generating plants then maps plants to type.
        """

        therms = []
        unmapped = set()
        for hour in df.index.values:
            dt = hour
            currentt = df.loc[[hour]]

            # Create current plant output.
            tp = {}
            for item in list(df):
                v = currentt.iloc[0][item]
                tp[item] = v

            current_plants = {k: tp[k] for k in tp if not isnan(tp[k])}

            for plant in current_plants.keys():
                if plant not in thermal_plants.keys():
                    unmapped.add(plant)

            mapped_plants = [
                (thermal_plants.get(plant, "unknown"), val)
                for plant, val in current_plants.items()
            ]

            thermalDict = defaultdict(lambda: 0.0)

            # Sum values for duplicate keys.
            for key, val in mapped_plants:
                thermalDict[key] += val

            thermalDict["datetime"] = dt
            thermalDict = dict(thermalDict)
            therms.append(thermalDict)

        for plant in unmapped:
            logger.warning(
                "{} is missing from the DO plant mapping!".format(plant),
                extra={"key": "DO"},
            )

        return therms


    def total_production(self,df) -> List[dict]:
        """Takes DataFrame and finds generation totals for each hour."""

        vals = []
        # The Dominican Republic does not observe daylight savings time.
        for hour in df.index.values:
            dt = hour
            current = df.loc[[hour]]
            hydro = current.iloc[0]["Hydro"]
            wind = current.iloc[0]["Wind"]
            solar = current.iloc[0]["Solar"]
            if wind > -10:
                wind = max(wind, 0)

            # Wind and hydro totals do not always update exactly on the new hour.
            # In this case we set them to None because they are unknown rather than zero.
            if isnan(wind):
                wind = None
            if isnan(hydro):
                hydro = None

            prod = {"wind": wind, "hydro": hydro, "solar": solar, "datetime": dt}
            vals.append(prod)

        return vals


    def merge_production(self,thermal, total) -> List[dict]:
        """
        Takes thermal generation and total generation and merges them using 'datetime' key.
        """

        d = defaultdict(dict)
        for each in (thermal, total):
            for elem in each:
                d[elem["datetime"]].update(elem)

        final = sorted(d.values(), key=itemgetter("datetime"))

        def get_datetime(hour):
            at = arrow.now("America/Dominica").floor("day")
            dt = (at.shift(hours=int(hour) - 1)).datetime
            return dt

        for item in final:
            i = item["datetime"]
            j = get_datetime(i)
            item["datetime"] = j

        return final


    def fetch_production(self,
        zone_key: str = "DO",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> List[dict]:
        """Requests the last known production mix (in MW) of a given country."""
        if target_datetime:
            raise NotImplementedError("This parser is not yet able to parse past dates")

        dat = self.data_formatter(self.get_data_DO(session=session))
        tot = self.data_parser(dat["totals"])
        th = self.data_parser(dat["thermal"])
        thermal = self.thermal_production(th, logger)
        total = self.total_production(tot)
        merge = self.merge_production(thermal, total)

        production_mix_by_hour = []
        for hour in merge:
            production_mix = {
                "zoneKey": zone_key,
                "datetime": hour["datetime"],
                "production": {
                    "biomass": hour.get("biomass", 0.0),
                    "coal": hour.get("coal", 0.0),
                    "gas": hour.get("gas", 0.0),
                    "hydro": hour.get("hydro", 0.0),
                    "nuclear": 0.0,
                    "oil": hour.get("oil", 0.0),
                    "solar": hour.get("solar", 0.0),
                    "wind": hour.get("wind", 0.0),
                    "geothermal": 0.0,
                    "unknown": hour.get("unknown", 0.0),
                },
                "storage": {
                    "hydro": None,
                },
                "source": "oc.org.do",
            }
            production_mix_by_hour.append(production_mix)

        return production_mix_by_hour


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    s = extract_data()
    print("fetch_production() ->")
    print(s.fetch_production())
