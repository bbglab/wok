###############################################################################
#
#    Copyright 2009-2011, Universitat Pompeu Fabra
#
#    This file is part of Wok.
#
#    Wok is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Wok is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses
#
###############################################################################

import os.path
import json

from wok import VERSION

from data import DataElement, Data

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

class OptionsConfig(DataElement):
	"""
	Command line options parser and configuration loader.

	It parses the arguments, loads configuration files (with -c option)
	and appends new configuration parameters (with -D option)

	Constructor:

	initial_conf: Base configuration
	required: A list of required configuration keys
	args_usage: The command line arguments usage help string
	add_options: A function that will be called to populate the OptionParser with new options
	             Example:
				    def more_options(parser):
						parser.add_option("-o", "--output", dest="output")

					c = OptionsConfig(add_options = more_options)
					print c.options.output
	expand_vars: whether to expand references like ${key1} or not

	Attributes:

	options: The option variables got from the OptionParser
	args: The arguments got from the OptionParser
	builder: The configuration builder
	"""
	
	def __init__(self, initial_conf_files = None, initial_conf = None, required = [], args_usage = "", add_options = None, expand_vars = False):
		DataElement.__init__(self)
		
		from optparse import OptionParser

		parser = OptionParser(usage = "usage: %prog [options] " + args_usage, version = VERSION)

		parser.add_option("-L", "--log-level", dest="log_level", 
			default=None, choices=["debug", "info", "warn", "error", "critical", "notset"],
			help="Which log level: debug, info, warn, error, critical, notset")

		parser.add_option("-P", "--project", action="append", dest="projects", default=[], metavar="PATH",
			help="Include this project. Multiple projects can be specified.")

		parser.add_option("-C", "--conf", action="append", dest="conf_files", default=[], metavar="FILE",
			help="Load configuration from a file. Multiple files can be specified")
			
		parser.add_option("-D", action="append", dest="data", default=[], metavar="PARAM=VALUE",
			help="External data value. example -D param1=value")

		if add_options is not None:
			add_options(parser)

		(self.options, self.args) = parser.parse_args()

		self.builder = ConfigBuilder()

		if initial_conf is not None:
			if isinstance(initial_conf, dict):
				initial_conf = Data.create(initial_conf)
			self.builder.add_merge(initial_conf)

		"""
		if self.options.log_level is not None:
			self.builder.add_value("wok.log.level", self.options.log_level)
			self.builder.add_value("wok.platform.log.level", self.options.log_level)
			self.builder.add_value("wok.platform.jobs.log.level", self.options.log_level)
		"""

		# Load configuration

		conf_files = []
		if initial_conf_files is not None:
			conf_files.extend(initial_conf_files)
		conf_files.extend(self.options.conf_files)	
		
		if len(conf_files) > 0:
			files = []
			for conf_file in conf_files:
				self.builder.add_file(conf_file)
				files.append(os.path.abspath(conf_file))

			self.builder.add_value("__files", Data.create(files))

		for data in self.options.data:
			d = data.split("=")
			if len(d) != 2:
				raise Exception("Data argument wrong: " + data)

			self.builder.add_value(d[0], d[1])

		# Projects

		self.builder.add_element("wok.projects", self.options.projects)

		# Build configuration

		self.builder.merge_into(self)

		if len(required) > 0:
			self.check_required(required)

		if expand_vars:
			self.expand_vars()

	def check_required(self, required):
		for name in required:
			if not name in self:
				raise Exception("Missing required configuration: %s" % name)