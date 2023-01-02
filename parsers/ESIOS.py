#!/usr/bin/env python3

from datetime import datetime
from logging import Logger, getLogger
from typing import Optional
from urllib.parse import urlencode

# The arrow library is used to handle datetimes
import arrow
from requests import Response, Session

from .lib.exceptions import ParserException
from .lib.utils import get_token
from parsers.func import get_data

class get_data_ESIOS(get_data):
    def get_data_warn(self,session=None,url:str=" ",Format:str = None,target_datetime=None,header={}):
        if target_datetime is not None:
            raise NotImplementedError("This parser is not yet able to parse past dates")
        r= session or Session()
        r = r.get(url,header=header)
        return r

reader = get_data_ESIOS()

def fetch_exchange(
    zone_key1: str = "ES",
    zone_key2: str = "MA",
    session: Optional[Session] = None,
    target_datetime: Optional[datetime] = None,
    logger: Logger = getLogger(__name__),
) -> list:



    # Get ESIOS token
    token = get_token("ESIOS_TOKEN")



    # Request headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json; application/vnd.esios-api-v2+json",
        "Authorization": 'Token token="{0}"'.format(token),
    }

    # Request query url
    utc = arrow.utcnow()
    start_date = utc.shift(hours=-24).floor("hour").isoformat()
    end_date = utc.ceil("hour").isoformat()
    dates = {"start_date": start_date, "end_date": end_date}
    query = urlencode(dates)
    url = "https://api.esios.ree.es/indicators/10209?{0}".format(query)

    response: Response = reader.get_data_warn(session=session,url=url,header=headers)
    if response.status_code != 200 or not response.text:
        raise ParserException(
            "ESIOS", "Response code: {0}".format(response.status_code)
        )

    json = response.json()
    values = json["indicator"]["values"]
    if not values:
        raise ParserException("ESIOS", "No values received")
    else:
        data = []
        sorted_zone_keys = sorted([zone_key1, zone_key2])

        for value in values:
            # Get last value in datasource
            datetime = arrow.get(value["datetime_utc"]).datetime
            # Datasource negative value is exporting, positive value is importing
            net_flow = -value["value"]

            value_data = {
                "sortedZoneKeys": "->".join(sorted_zone_keys),
                "datetime": datetime,
                "netFlow": net_flow
                if zone_key1 == sorted_zone_keys[0]
                else -1 * net_flow,
                "source": "api.esios.ree.es",
            }

            data.append(value_data)

        return data


if __name__ == "__main__":
    session = Session()
    print(fetch_exchange("ES", "MA", session))
