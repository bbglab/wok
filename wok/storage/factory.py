from wok.core.plugin import PluginFactory

from files import FilesStorage

_class_list = [FilesStorage]

try:
	from gridfs import GridFsStorage
	_class_list += [GridFsStorage]
except:
	pass

storage_factory = PluginFactory("storage", _class_list)