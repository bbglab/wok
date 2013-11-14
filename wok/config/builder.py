import os.path
import json

from data import Data
from loader import ConfigLoader

class ConfigMerge(object):
	def __init__(self, element):
		self.element = element

	def merge_into(self, conf):
		conf.merge(self.element)

	def __repr__(self):
		return "[{:8s}] {}".format("Merge", repr(self.element))

class ConfigValue(object):
	def __init__(self, key, value, merge=False):
		self.key = key
		self.value = value
		self._merge = merge

	def merge_into(self, conf):
		try:
			v = json.loads(self.value)
		except:
			v = self.value
		if self._merge and self.key in conf and Data.is_element(conf[self.key]):
			conf[self.key].merge(self.value)
		else:
			conf[self.key] = Data.create(v)

	def __repr__(self):
		return "[{:8s}] {} = {}{}".format("Value", self.key, self.value, " (merge)" if self._merge else "")

class ConfigFile(object):
	def __init__(self, path):
		self.path = os.path.abspath(path)

	def merge_into(self, conf):
		loader = ConfigLoader(self.path)
		conf.merge(loader.load())

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

	def add_value(self, key, value, merge=True):
		self.__parts += [ConfigValue(key, value, merge)]

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
