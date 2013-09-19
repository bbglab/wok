import os.path
import json

from data import Data

class ConfigMerge(object):
	def __init__(self, element):
		self.element = element

	def merge_into(self, conf):
		conf.merge(self.element)

	def __repr__(self):
		return "[{:8s}] {}".format("Merge", repr(self.element))

class ConfigValue(object):
	def __init__(self, key, value):
		self.key = key
		self.value = value

	def merge_into(self, conf):
		try:
			v = json.loads(self.value)
		except:
			v = self.value
		conf[self.key] = Data.create(v)

	def __repr__(self):
		return "[{:8s}] {} = {}".format("Value", self.key, self.value)

class ConfigElement(object):
	def __init__(self, key, value):
		self.key = key
		self.value = value

	def merge_into(self, conf):
		if self.key in conf:
			conf[self.key].merge(self.value)
		else:
			conf[self.key] = self.value

	def __repr__(self):
		return "[{:8s}] {} = {}".format("Element", self.key, repr(self.value))

class ConfigFile(object):
	def __init__(self, path):
		self.path = os.path.abspath(path)

	def merge_into(self, conf):
		try:
			f = open(self.path, "r")
			v = json.load(f)
			cf = Data.create(v)
			conf.merge(cf)
			f.close()
		except Exception as e:
			from wok import logger
			msg = ["Error loading configuration from ",
					self.path, ":\n\n", str(e), "\n"]
			logger.get_logger("wok.config").error("".join(msg))
			raise

	def __repr__(self):
		return "[{:8s}] {}".format("File", self.path)

class ConfigBuilder(object):
	def __init__(self, conf_builder=None):
		if conf_builder is None:
			self.__parts = []
		else:
			self.__parts = [p for p in conf_builder.__parts]

	def add_merge(self, element):
		self.__parts += [ConfigMerge(element)]

	def add_value(self, key, value):
		self.__parts += [ConfigValue(key, value)]

	def add_element(self, key, value):
		self.__parts += [ConfigElement(key, value)]

	def add_file(self, path):
		self.__parts += [ConfigFile(path)]

	def add_builder(self, builder):
		self.__parts += [builder]

	def merge_into(self, conf):
		for part in self.__parts:
			part.merge_into(conf)

	def get_conf(self, conf=None):
		if conf is None:
			conf = Data.element()
		self.merge_into(conf)
		return conf

	def __call__(self, conf=None):
		return self.get_conf(conf)

	def __repr__(self):
		sb = ["ConfigBuilder:"]
		for part in self.__parts:
			sb += ["  {}".format(repr(part))]
		return "\n".join(sb)
