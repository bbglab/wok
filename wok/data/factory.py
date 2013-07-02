from wok import logger

logger.get_logger("wok.data")

_DATA_PROVIDERS = {}

try:
	from mongo import MongoProvider
	_DATA_PROVIDERS["mongo"] = MongoProvider
except Exception as ex:
	logger.warn("MondoDB data provider not available.")
	logger.exception(ex)

def create_data_provider(name, conf):
	if name not in _DATA_PROVIDERS:
		raise Exception("Data provider not available: {}".format(name))

	conf["type"] = name

	return _DATA_PROVIDERS[name](conf)