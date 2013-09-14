from wok import logger

from files import FilesProvider

_DATA_PROVIDERS = {
	"files" : FilesProvider
}

def __log(msg, level=None):
	from wok.logger import get_logger
	log = get_logger(name="wok.data")

	if level is None:
		import logging
		level = logging.WARNING
	log.log(level, msg)

try:
	from mongo import MongoProvider
	_DATA_PROVIDERS["mongo"] = MongoProvider
except ImportError:
	__log("The MongoDB data provider can not be loaded.")
	__log("This data provider is necessary only if you are going to use a MongoDB database.")
	__log("To install it run the following command: pip install pymongo-2.5.2")

def create_data_provider(name, conf):
	if name not in _DATA_PROVIDERS:
		raise Exception("Data provider not available: {}".format(name))

	conf["type"] = name

	return _DATA_PROVIDERS[name](conf)