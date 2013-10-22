from wok.core.plugin import PluginFactory

from files import FilesStorage
from gridfs import GridFsStorage

storage_factory = PluginFactory("storage", [
	FilesStorage,
	GridFsStorage
])