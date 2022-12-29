#!/usr/bin/env python3

import json
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from logging import Logger, getLogger
from typing import Optional

import arrow
import pandas as pd
from requests import Session

from parsers.lib.config import refetch_frequency

from .lib.utils import get_token
from .lib.validation import validate, validate_production_diffs
from parsers.func import get_data
class get_data_FR(get_data):
    def get_data(self,session=None,url:str=" ",Format = None,pasmer = {}):
        r= session or Session()
        r = r.get(url,params=pasmer)
        return r.content

reader = get_data_FR()

API_ENDPOINT = "https://opendata.reseaux-energies.fr/api/records/1.0/search/"

MAP_GENERATION = {
    "nucleaire": "nuclear",
    "charbon": "coal",
    "gaz": "gas",
    "fioul": "oil",
    "eolien": "wind",
    "solaire": "solar",
    "bioenergies": "biomass",
}

MAP_HYDRO = [
    "hydraulique_fil_eau_eclusee",
    "hydraulique_lacs",
    "hydraulique_step_turbinage",
    "pompage",
]


def is_not_nan_and_truthy(v) -> bool:
    if isinstance(v, float) and math.isnan(v):
        return False
    return bool(v)


@refetch_frequency(timedelta(days=1))
def fetch_production(
    zone_key: str = "FR",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> list:
    if target_datetime:
        to = arrow.get(target_datetime, "Europe/Paris")
    else:
        to = arrow.now(tz="Europe/Paris")

    # setup request

    formatted_from = to.shift(days=-1).format("YYYY-MM-DDTHH:mm")
    formatted_to = to.format("YYYY-MM-DDTHH:mm")

    params = {
        "dataset": "eco2mix-national-tr",
        "q": "date_heure >= {} AND date_heure <= {}".format(
            formatted_from, formatted_to
        ),
        "timezone": "Europe/Paris",
        "rows": 100,
    }

    params["apikey"] = get_token("RESEAUX_ENERGIES_TOKEN")

    # make request and create dataframe with response
    response = reader.get_data(session=session,url=API_ENDPOINT,pasmer=params)

    data = json.loads(response)
    data = [d["fields"] for d in data["records"]]
    df = pd.DataFrame(data)

    # filter out desired columns and convert values to float
    value_columns = list(MAP_GENERATION.keys()) + MAP_HYDRO
    missing_fuels = [v for v in value_columns if v not in df.columns]
    present_fuels = [v for v in value_columns if v in df.columns]
    if len(missing_fuels) == len(value_columns):
        logger.warning("No fuels present in the API response")
        return list()
    elif len(missing_fuels) > 0:
        mf_str = ", ".join(missing_fuels)
        logger.warning(
            "Fuels [{}] are not present in the API " "response".format(mf_str)
        )

    df = df.loc[:, ["date_heure"] + present_fuels]
    df[present_fuels] = df[present_fuels].astype(float)

    datapoints = list()
    for row in df.iterrows():
        production = dict()
        for key, value in MAP_GENERATION.items():
            if key not in present_fuels:
                continue

            if -50 < row[1][key] < 0:
                # set small negative values to 0
                logger.warning("Setting small value of %s (%s) to 0." % (key, value))
                production[value] = 0
            else:
                production[value] = row[1][key]

        # Hydro is a special case!
        has_hydro_production = all(
            i in df.columns for i in ["hydraulique_lacs", "hydraulique_fil_eau_eclusee"]
        )
        has_hydro_storage = all(
            i in df.columns for i in ["pompage", "hydraulique_step_turbinage"]
        )
        if has_hydro_production:
            production["hydro"] = (
                row[1]["hydraulique_lacs"] + row[1]["hydraulique_fil_eau_eclusee"]
            )
        if has_hydro_storage:
            storage = {
                "hydro": row[1]["pompage"] * -1
                + row[1]["hydraulique_step_turbinage"] * -1
            }
        else:
            storage = dict()

        # if all production values are null, ignore datapoint
        if not any([is_not_nan_and_truthy(v) for k, v in production.items()]):
            continue

        datapoint = {
            "zoneKey": zone_key,
            "datetime": arrow.get(row[1]["date_heure"]).datetime,
            "production": production,
            "storage": storage,
            "source": "opendata.reseaux-energies.fr",
        }
        datapoint = validate(datapoint, logger, required=["nuclear", "hydro", "gas"])
        datapoints.append(datapoint)

    max_diffs = {
        "hydro": 1600,
        "solar": 500,
        "coal": 500,
        "wind": 1000,
        "nuclear": 1300,
    }

    datapoints = validate_production_diffs(datapoints, max_diffs, logger)

    return datapoints


if __name__ == "__main__":
    print(fetch_production())
