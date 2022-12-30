#!/usr/bin/env python3
"""
Parser for U.S. Energy Information Administration, https://www.eia.gov/ .

Aggregates and standardizes data from most of the US ISOs,
and exposes them via a unified API.

Requires an API key, set in the EIA_KEY environment variable. Get one here:
https://www.eia.gov/opendata/register.php
"""
from datetime import datetime, timedelta
from logging import Logger, getLogger
from typing import Any, Dict, List, Optional

import arrow
from dateutil import parser, tz
from requests import Session

from parsers.ENTSOE import merge_production_outputs
from parsers.lib.config import refetch_frequency
from parsers.lib.utils import get_token
from parsers.lib.validation import validate
from parsers.func import get_data
from parsers.example import paeras_example
from parsers.EIA_data import *
reader = get_data()

class extract_data(paeras_example):
    @refetch_frequency(timedelta(days=1))
    def fetch_production(self,
        zone_key: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        return self._fetch(
            zone_key,
            PRODUCTION.format(REGIONS[zone_key]),
            session=session,
            target_datetime=target_datetime,
            logger=logger,
        )


    @refetch_frequency(timedelta(days=1))
    def fetch_consumption(self,
        zone_key: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        consumption = self._fetch(
            zone_key,
            CONSUMPTION.format(REGIONS[zone_key]),
            session=session,
            target_datetime=target_datetime,
            logger=logger,
        )
        for point in consumption:
            point["consumption"] = point.pop("value")

        return consumption


    @refetch_frequency(timedelta(days=1))
    def fetch_consumption_forecast(self,
        zone_key: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        return self._fetch(
            zone_key,
            CONSUMPTION_FORECAST.format(REGIONS[zone_key]),
            session=session,
            target_datetime=target_datetime,
            logger=logger,
        )


    @refetch_frequency(timedelta(days=1))
    def fetch_production_mix(self,
        zone_key: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        mixes = []
        for type, code in TYPES.items():
            url_prefix = PRODUCTION_MIX.format(REGIONS[zone_key], code)
            mix = self._fetch(
                zone_key,
                url_prefix,
                session=session,
                target_datetime=target_datetime,
                logger=logger,
            )

            # EIA does not currently split production from the Virgil Summer C
            # plant across the two owning/ utilizing BAs:
            # US-CAR-SCEG and US-CAR-SC,
            # but attributes it all to US-CAR-SCEG
            # Here we apply a temporary fix for that until EIA properly splits the production
            # This split can be found in the eGRID data,
            # https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid

            if zone_key == "US-CAR-SCEG" and type == "nuclear":
                for point in mix:
                    point.update({"value": point["value"] * (1 - SC_VIRGIL_OWNERSHIP)})

            # Integrate the supplier zones in the zones they supply

            supplying_zones = PRODUCTION_ZONES_TRANSFERS.get(zone_key, {})
            zones_to_integrate = {
                **supplying_zones.get("all", {}),
                **supplying_zones.get(type, {}),
            }
            for zone, percentage in zones_to_integrate.items():
                url_prefix = PRODUCTION_MIX.format(REGIONS[zone], code)
                additional_mix = self._fetch(
                    zone,
                    url_prefix,
                    session=session,
                    target_datetime=target_datetime,
                    logger=logger,
                )
                for point in additional_mix:
                    point.update({"value": point["value"] * percentage})
                mix = self._merge_production_mix([mix, additional_mix])
            if not mix:
                continue

            for point in mix:
                negative_threshold = NEGATIVE_PRODUCTION_THRESHOLDS_TYPE.get(
                    type, NEGATIVE_PRODUCTION_THRESHOLDS_TYPE["default"]
                )

                if (
                    type != "hydro"
                    and point["value"]
                    and 0 > point["value"] >= negative_threshold
                ):
                    point["value"] = 0

                if type == "hydro" and point["value"] and point["value"] < 0:
                    point.update(
                        {
                            "production": {},  # required by merge_production_outputs()
                            "storage": {type: point.pop("value")},
                        }
                    )
                else:
                    point.update(
                        {
                            "production": {type: point.pop("value")},
                            "storage": {},  # required by merge_production_outputs()
                        }
                    )

                # replace small negative values (>-5) with 0s This is necessary for solar
                point = validate(point, logger=logger, remove_negative=True)
            mixes.append(mix)

        if not mixes:
            logger.warning(f"No production mix data found for {zone_key}")
            return []

        # Some of the returned mixes could be for older timeframes.
        # Fx the latest oil data could be 6 months old.
        # In this case we want to discard the old data as we won't be able to merge it
        timeframes = [sorted(map(lambda x: x["datetime"], mix)) for mix in mixes]
        latest_timeframe = max(timeframes, key=lambda x: x[-1])

        correct_mixes = []
        for mix in mixes:
            correct_mix = []
            for production_in_mix in mix:
                if production_in_mix["datetime"] in latest_timeframe:
                    correct_mix.append(production_in_mix)
            if len(correct_mix) > 0:
                correct_mixes.append(correct_mix)

        return merge_production_outputs(correct_mixes, zone_key, merge_source="eia.gov")


    @refetch_frequency(timedelta(days=1))
    def fetch_exchange(seslf,
        zone_key1: str,
        zone_key2: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        sortedcodes = "->".join(sorted([zone_key1, zone_key2]))
        exchange = self._fetch(
            sortedcodes,
            url_prefix=EXCHANGE.format(EXCHANGES[sortedcodes]),
            session=session,
            target_datetime=target_datetime,
            logger=logger,
        )
        for point in exchange:
            point.update(
                {
                    "sortedZoneKeys": point.pop("zoneKey"),
                    "netFlow": point.pop("value"),
                }
            )
            if sortedcodes in REVERSE_EXCHANGES:
                point["netFlow"] = -point["netFlow"]

        return exchange


    def _fetch(self,
        zone_key: str,
        url_prefix: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ):
        # get EIA API key
        API_KEY = get_token("EIA_KEY")

        if target_datetime:
            try:
                target_datetime = arrow.get(target_datetime).datetime
            except arrow.parser.ParserError:
                raise ValueError(
                    f"target_datetime must be a valid datetime - received {target_datetime}"
                )
            utc = tz.gettz("UTC")
            eia_ts_format = "%Y-%m-%dT%H"
            end = target_datetime.astimezone(utc) + timedelta(hours=1)
            start = end - timedelta(days=1)
            url = f"{url_prefix}&api_key={API_KEY}&start={start.strftime(eia_ts_format)}&end={end.strftime(eia_ts_format)}"
        else:
            url = f"{url_prefix}&api_key={API_KEY}&sort[0][column]=period&sort[0][direction]=desc&length=24"

        raw_data = reader.get_data(session,url,"json")
        if raw_data.get("response", {}).get("data", None) is None:
            return []
        return [
            {
                "zoneKey": zone_key,
                "datetime": self._get_utc_datetime_from_datapoint(
                    parser.parse(datapoint["period"])
                ),
                "value": datapoint["value"],
                "source": "eia.gov",
            }
            for datapoint in raw_data["response"]["data"]
        ]


    def _index_by_timestamp(self,datapoints: List[dict]) -> Dict[str, dict]:
        indexed_data = {}
        for datapoint in datapoints:
            indexed_data[datapoint["datetime"]] = datapoint
        return indexed_data


    def _merge_production_mix(self,mixes: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        merged_data = {}
        for mix in mixes:
            indexed_mix = self._index_by_timestamp(mix)
            for timestamp, mix_value in indexed_mix.items():
                if not timestamp in merged_data.keys():
                    merged_data[timestamp] = mix_value
                else:
                    merged_data[timestamp]["value"] += mix_value["value"]
        return list(merged_data.values())


    def _conform_timestamp_convention(self,dt: datetime):
        # The timestamp given by EIA represents the end of the time interval.
        # ElectricityMap using another convention,
        # where the timestamp represents the beginning of the interval.
        # So we need shift the datetime 1 hour back.
        return dt - timedelta(hours=1)


    def _get_utc_datetime_from_datapoint(self,dt: datetime):
        """update to beginning hour convention and timezone to utc"""
        dt_beginning_hour = self._conform_timestamp_convention(dt)
        dt_utc = arrow.get(dt_beginning_hour).to("utc")
        return dt_utc.datetime


if __name__ == "__main__":
    from pprint import pprint

    # pprint(fetch_production('US-CENT-SWPP'))
    # # pprint(fetch_consumption_forecast('US-CAL-CISO'))
    pprint(
        fetch_exchange(
            zone_key1="US-CENT-SWPP",
            zone_key2="CA-SK",
            target_datetime=datetime(2022, 3, 1),
        )
    )
