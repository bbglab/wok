from wok import logger

from errors import MissingConfigParamsError

class PluginError(Exception):
	pass

class Plugin(object):

	# static variables:
	#
	# plugin_type: The name of the plugin used by the factory
	# plugin_required_conf: The list of required configuration parameters

	def __init__(self, conf, logger_name=None):
		self._conf = conf
		if hasattr(self, "plugin_required_conf"):
			missing_conf = []
			for arg in self.plugin_required_conf:
				if arg not in conf:
					missing_conf += [arg]
			if len(missing_conf) > 0:
				raise MissingConfigParamsError(missing_conf)

		if logger_name is not None:
			self._log = logger.get_logger(logger_name)

	def context_conf(self, ctx_name):
		conf = self._conf.clone()
		if ctx_name in conf:
			ctx_conf = conf[ctx_name]
			del conf[ctx_name]
			conf.merge(ctx_conf)
		if "type" not in conf:
			conf["type"] = self.plugin_type
		return conf

class PluginFactory(object):

	def __init__(self, category, class_list, default=None):
		self.__category = category
		self.__class_list = class_list
		self.__default = default
		self.__class_map = {}
		for plugin_class in class_list:
			if not hasattr(plugin_class, "plugin_type"):
				raise PluginError("Plugin '{}' missing 'plugin_type' attribute".format(plugin_class.__name__))
			self.__class_map[plugin_class.plugin_type] = plugin_class

	def create(self, conf=None, type=None, logger=None):
		type = type or conf.get("type") or self.__default
		if type is None:
			raise PluginError("Missing {} type".format(self.__category))

		if type not in self.__class_map:
			raise PluginError("Unknown {}: {}".format(self.__category, type))

		if logger is not None:
			logger.info("Creating '{}' {} ...".format(type, self.__category))
			logger.debug("{} configuration: {}".format(self.__category.title(), repr(conf)))

		return self.__class_map[type](conf)