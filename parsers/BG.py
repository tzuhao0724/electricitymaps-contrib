#!/usr/bin/env python3
# coding=utf-8


from logging import Logger, getLogger
from typing import Optional

import arrow
from requests import Session
from parsers.func import get_data
from parsers.example import paeras_example
reader = get_data()
TYPE_MAPPING = {  # Real values around midnight
    "АЕЦ": "nuclear",  # 2000
    "Кондензационни ТЕЦ": "coal",  # 1800
    "Топлофикационни ТЕЦ": "gas",  # 146
    "Заводски ТЕЦ": "gas",  # 147
    "ВЕЦ": "hydro",  # 7
    "Малки ВЕЦ": "hydro",  # 74
    "ВяЕЦ": "wind",  # 488
    "ФЕЦ": "solar",  # 0
    "Био ТЕЦ": "biomass",  # 29
    "Био ЕЦ": "biomass",  # 29
    "Товар РБ": "consumption",  # 3175
}

class extract_data(paeras_example):
    def fetch_production(self,
        zone_key: str = "BG",
        session: Optional[Session] = None,
        target_datetime=None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Requests the last known production mix (in MW) of a given country."""
        url = "http://www.eso.bg/api/rabota_na_EEC_json.php"
        res = reader.get_data_warn(session,url,target_datetime=target_datetime)
        assert (
            res.status_code == 200
        ), f"Exception when fetching production for {zone_key}: error when calling url={url}"

        response = res.json()

        logger.debug(f"Raw generation breakdown: {response}")

        datapoints = []
        for row in response:
            for k in TYPE_MAPPING.keys():
                if row[0].startswith(k):
                    datapoints.append((TYPE_MAPPING[k], row[1]))
                    break

        production = {}
        for k, v in datapoints:
            production[k] = production.get(k, 0.0) + v

        data = {
            "zoneKey": zone_key,
            "production": production,
            "storage": {},
            "source": "eso.bg",
            "datetime": arrow.utcnow().datetime,
        }

        return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    s = extract_data()
    print("fetch_production() ->")
    print(s.fetch_production())
