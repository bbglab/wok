from wok import logger
from wok.core.plugin import PluginFactory

from files import FilesProvider

_class_list = [FilesProvider]

def __log(msg, level=None):
	from wok.logger import get_logger
	log = get_logger("wok.data")

	if level is None:
		import logging
		level = logging.WARNING
	log.log(level, msg)

try:
	from mongo import MongoProvider
	_class_list += [MongoProvider]
except ImportError:
	__log("The MongoDB data provider can not be loaded.")
	__log("This data provider is necessary only if you are going to use a MongoDB database.")
	__log("To install it run the following command: pip install pymongo-2.5.2")

data_provider_factory = PluginFactory("data provider", _class_list)
