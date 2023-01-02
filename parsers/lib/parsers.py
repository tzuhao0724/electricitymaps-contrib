import importlib

from electricitymap.contrib.config import EXCHANGES_CONFIG, ZONES_CONFIG

# Prepare all parsers
CONSUMPTION_PARSERS = {}
PRODUCTION_PARSERS = {}
PRODUCTION_PER_MODE_FORECAST_PARSERS = {}
PRODUCTION_PER_UNIT_PARSERS = {}
EXCHANGE_PARSERS = {}
PRICE_PARSERS = {}
CONSUMPTION_FORECAST_PARSERS = {}
GENERATION_FORECAST_PARSERS = {}
EXCHANGE_FORECAST_PARSERS = {}

PARSER_KEY_TO_DICT = {
    "consumption": CONSUMPTION_PARSERS,
    "production": PRODUCTION_PARSERS,
    "productionPerUnit": PRODUCTION_PER_UNIT_PARSERS,
    "productionPerModeForecast": PRODUCTION_PER_MODE_FORECAST_PARSERS,
    "exchange": EXCHANGE_PARSERS,
    "price": PRICE_PARSERS,
    "consumptionForecast": CONSUMPTION_FORECAST_PARSERS,
    "generationForecast": GENERATION_FORECAST_PARSERS,
    "exchangeForecast": EXCHANGE_FORECAST_PARSERS,
}



class data_extracter():
    def Dict_build(self,object):
        for id, config in object.items():
            for parser_key, v in config.get("parsers", {}).items():
                mod_name, fun_name = v.split(".")
                mod = importlib.import_module("parsers.%s" % mod_name)
                mod = mod.extract_data()
                PARSER_KEY_TO_DICT[parser_key][id] = getattr(mod, fun_name)


extracter = data_extracter()
# Read all zones
extracter.Dict_build(ZONES_CONFIG)

# Read all exchanges
extracter.Dict_build(EXCHANGES_CONFIG)

