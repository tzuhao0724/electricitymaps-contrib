from datetime import datetime
from logging import Logger, getLogger
from typing import Optional, Union

import arrow

# Numpy and PIL are used to process the image
import numpy as np
from PIL import Image
from requests import Session
from parsers.func import get_data
from AX_data import *
from parsers.example import paeras_example
URL = "http://194.110.178.135/grafik/stamnat.php"
SOURCE = "kraftnat.ax"
TZ = "Europe/Mariehamn"
class get_data_AX(get_data):
    def get_data(self,session=None,url:str=" ",Format:str = None):
        r= session or Session()
        r = r.get(url,stream=True).raw
        return r
class extract_data(paeras_example):
    def _get_masks(self,session=None):
        shorts = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        masks = [Minus, Dot, Zero, One, Two, Three, Four, Five, Six, Seven, Eight, Nine]

        return dict(zip(shorts, masks))


    def _fetch_data(self,session: Optional[Session] = None) -> dict:
        """Return usable data from source."""
        # Load masks for reading numbers from the image
        # Create a dictionary of symbols and their pixel masks
        mapping = self._get_masks(session)

        # Download the updating image from Kraftnät Åland

        reader = get_data_AX()
        r = reader.get_data_warn(session,URL)

        im = Image.open(r)
        # Get timestamp
        fetchtime = arrow.utcnow().floor("second").to(TZ)

        # "data" is a height x width x 3 RGB numpy array
        data = np.array(im)
        # red, green, blue, alpha = data.T
        # Temporarily unpack the bands for readability
        red, green, blue = data.T
        # Color non-blue areas in the image with white
        blue_areas = (red == 0) & (green == 0) & (blue == 255)
        data[~blue_areas.T] = (255, 255, 255)
        # Color blue areas in the image with black
        data[blue_areas.T] = (0, 0, 0)

        # Transform the array back to image
        im = Image.fromarray(data)

        shorts = mapping.keys()

        # check import from Sweden
        se_3_flow = []
        for x in range(80, 130 - 6):
            for abr in shorts:
                im1 = im.crop((x, 443, x + 6, 452))
                if im1 == mapping[abr]:
                    se_3_flow.append(abr)
        se_3_flow = "".join(se_3_flow)
        se_3_flow = round(float(se_3_flow), 1)

        # export Åland-Finland(Kustavi/Gustafs)
        gustafs_flow = []
        for x in range(780, 825 - 6):
            for abr in shorts:
                im1 = im.crop((x, 43, x + 6, 52))
                if im1 == mapping[abr]:
                    gustafs_flow.append(abr)
        gustafs_flow = "".join(gustafs_flow)
        gustafs_flow = round(float(gustafs_flow), 1)

        # Reserve cable import Naantali-Åland
        # Åland administration does not allow export
        # to Finland through this cable
        fin_flow = []
        for x in range(760, 815 - 6):
            for abr in shorts:
                im1 = im.crop((x, 328, x + 6, 337))
                if im1 == mapping[abr]:
                    fin_flow.append(abr)
        fin_flow = "".join(fin_flow)
        fin_flow = round(float(fin_flow), 1)

        # The shown total consumption is not reliable according to the TSO
        # Consumption
        # Cons = []
        # for x in range(650, 700-6):
        #    for abr in shorts:
        #        im1 = im.crop((x, 564, x+6, 573))
        #        if im1 == mapping[abr]:
        #            Cons.append(abr)
        # Cons = "".join(Cons)
        # Cons = round(float(Cons),1)

        # Wind production
        wind = []
        for x in range(650, 700 - 6):
            for abr in shorts:
                im1 = im.crop((x, 576, x + 6, 585))
                if im1 == mapping[abr]:
                    wind.append(abr)
        wind = "".join(wind)
        wind = round(float(wind), 1)

        # Fossil fuel production
        fossil = []
        for x in range(650, 700 - 6):
            for abr in shorts:
                im1 = im.crop((x, 588, x + 6, 597))
                if im1 == mapping[abr]:
                    fossil.append(abr)
        fossil = "".join(fossil)
        fossil = round(float(fossil), 1)

        # Both are confirmed to be import from Finland by the TSO
        fin_flow = fin_flow + gustafs_flow

        # Calculate sum of exchanges
        sum_exchanges = se_3_flow + fin_flow
        # Calculate total production
        total_production = fossil + wind
        # Calculate total consumption
        consumption = round(total_production + sum_exchanges, 1)

        # The production that is not fossil fuel or wind based is unknown
        # Impossible to estimate with current data
        # UProd = TotProd - WProd - FProd
        return {
            "production": total_production,
            "consumption": consumption,
            "wind": wind,
            "fossil": fossil,
            "SE3->AX": se_3_flow,
            "FI->AX": fin_flow,
            "fetchtime": fetchtime,
        }


    def fetch_production(self,
        zone_key: str = "AX",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Requests the last known production mix (in MW) of a given country."""
        if target_datetime:
            raise NotImplementedError("This parser is not yet able to parse past dates")

        obj = self._fetch_data(session)

        return {
            "zoneKey": zone_key,
            "production": {
                "biomass": None,
                "coal": 0,
                "gas": 0,
                "hydro": None,
                "nuclear": 0,
                "oil": obj["fossil"],
                "solar": None,
                "wind": obj["wind"],
                "geothermal": None,
                "unknown": None,
            },
            "storage": {},
            "source": SOURCE,
            "datetime": arrow.get(obj["fetchtime"]).datetime,
        }


    def fetch_consumption(self,
        zone_key: str = "AX",
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        if target_datetime:
            raise NotImplementedError("This parser is not yet able to parse past dates")

        obj = self._fetch_data(session)

        return {
            "zoneKey": zone_key,
            "datetime": arrow.get(obj["fetchtime"]).datetime,
            "consumption": obj["consumption"],
            "source": SOURCE,
        }


    def fetch_exchange(self,
        zone_key1: str,
        zone_key2: str,
        session: Optional[Session] = None,
        target_datetime: Optional[datetime] = None,
        logger: Logger = getLogger(__name__),
    ) -> dict:
        """Requests the last known power exchange (in MW) between two countries."""
        if target_datetime:
            raise NotImplementedError("This parser is not yet able to parse past dates")

        obj = self._fetch_data(session)

        # Country codes are sorted in order to enable easier indexing in the database
        sorted_zone_keys = "->".join(sorted([zone_key1, zone_key2]))

        data = {
            "sortedZoneKeys": sorted_zone_keys,
            "source": SOURCE,
            "datetime": arrow.get(obj["fetchtime"]).datetime,
        }

        # Here we assume that the net flow returned by the api is the flow from
        # country1 to country2. A positive flow indicates an export from country1
        # to country2. A negative flow indicates an import.
        net_flow: Union[float, None] = None
        if sorted_zone_keys in ["AX->SE", "AX->SE-SE3"]:
            net_flow = obj["SE3->AX"]

        elif sorted_zone_keys == "AX->FI":
            net_flow = obj["FI->AX"]  # Import is positive

        # The net flow to be reported should be from the first country to the second
        # (sorted alphabetically). This is NOT necessarily the same direction as the flow
        # from country1 to country2

        #  AX is before both FI and SE
        if net_flow:
            data["netFlow"] = round(-1 * net_flow, 1)
        else:
            data["netFlow"] = None

        return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    s = extract_data()
    print("fetch_production() ->")
    print(s.fetch_production())
    print("fetch_consumption() ->")
    print(s.fetch_consumption())
    print("fetch_exchange(AX, FI) ->")
    print(s.fetch_exchange("FI", "AX"))
    print("fetch_exchange(AX, SE-SE3) ->")
    print(s.fetch_exchange("SE-SE3", "AX"))
