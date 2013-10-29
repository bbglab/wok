import os
import re
import json

from wok import logger as woklogger
from wok.config.builder import ConfigFile
from wok.config.data import Data
from wok.core.flow.loader import FlowLoader

_LOGGER_NAME = "wok.projects"

class ConfRule(object):
	def __init__(self, rule, base_path=None):
		self.on = rule.get("on", {})
		if isinstance(self.on, basestring):
			self.on = dict(task=self.on)

		self.dels = rule.get("del", default=Data.list)
		if not Data.is_list(self.dels):
			raise Exception("Expected a list of strings for del operations of rule: {}".format(repr(rule)))
		for k in self.dels:
			if not isinstance(k, basestring):
				raise Exception("Expected a list of strings for del operations of rule: {}".format(repr(rule)))

		self.set = rule.get("set", default=Data.list)
		if not Data.is_list(self.dels):
			raise Exception("Expected a list of tuples [key, value] for set operations of rule: {}".format(repr(rule)))
		for s in self.set:
			if not Data.is_list(s) or len(s) != 2:
				raise Exception("Expected a list of tuples [key, value] for set operations of rule: {}".format(repr(rule)))

		self.merge = rule.get("merge")
		if isinstance(self.merge, basestring):
			if not os.path.isabs(self.merge):
				if base_path is None:
					raise Exception("Configuration rule merge path should be absolute path: {}".format(self.merge))
				else:
					self.merge = os.path.join(base_path, self.merge)
			if not os.path.isfile(self.merge):
				raise Exception("Configuration rule merge path not found: {}".format(self.merge))
			with open(self.merge, "r") as f:
				self.merge = Data.create(json.load(f))
		if not Data.is_element(self.merge):
			raise Exception("Expected a dictionary for merge operation of rule: {}".format(repr(rule)))

	def match(self, **kwargs):
		for key, value in self.on.items():
			if key not in kwargs:
				return False

			if len(value) >= 2 and value[0] == "/" and value[-1] == "/": # regex
				ok = re.match(value[1:-1], kwargs[key])
			else: # string comparison
				ok = value == kwargs[key]

			if not ok:
				return False

		return True

	def apply(self, conf):
		# apply dels
		for key in self.dels:
			if key in conf:
				del conf[key]

		# apply sets
		for key, value in self.set:
			conf[key] = value

		# apply merges
		conf.merge(self.merge)

class PlatformContext(object):
	def __init__(self, desc):
		self.platform = desc.get("platform")
		self.path = desc.get("path")
		self.conf = desc.get("conf", default=Data.element)
		self.conf_rules = [ConfRule(rule) for rule in desc.get("conf_rules", default=Data.list)]

	def merge(self,desc):
		self.path = self.path or desc.get("path")
		self.conf.merge(desc.get("conf"))
		self.conf_rules += [ConfRule(rule) for rule in desc.get("conf_rules", default=Data.list)]
		self.flows += desc.get("flows", default=Data.list)

	def _repr(self, sb, indent=0):
		sb += [" " * indent, "Platform [{}]".format(self.platform)]
		sb += [" " * indent, "  path={0}".format(self.path)]
		sb += [" " * indent, "  conf={0}".format(self.conf)]
		sb += [" " * indent, "  conf_rules={0}".format(self.conf_rules)]
		return sb

class Project(object):
	def __init__(self, desc, name=None, logger=None, base_path=None):
		self._log = logger or woklogger.get_logger(_LOGGER_NAME)

		self.name = name or desc.get("name")

		self._flow_loader = None

		self._platforms = {}

		if "platform" not in desc:
			self.path = desc.get("path")
			self.conf = desc.get("conf", default=Data.element)
			self.conf_rules = [ConfRule(rule, base_path) for rule in desc.get("conf_rules", default=Data.list)]
			self.flows = desc.get("flows", default=Data.list)
		else:
			self.conf = Data.element()
			self.conf_rules = Data.list()
			self.flows = Data.list()
			self._platforms[desc["platform"]] = PlatformContext(desc)

	def merge(self, desc, base_path=None):
		if "platform" not in desc:
			self.path = self.path or desc.get("path")
			self.conf.merge(desc.get("conf"))
			self.conf_rules += [ConfRule(rule, base_path) for rule in desc.get("conf_rules", default=Data.list)]
			self.flows += desc.get("flows", default=Data.list)
		else:
			platform_name = desc["platform"]
			if platform_name not in self._platforms:
				self._platforms[platform_name] = PlatformContext(desc)
			else:
				self._platforms[platform_name].merge(desc)

	def initialize(self):
		self._log.info("Initializing project {} ...".format(self.name))

		if self.path is None:
			raise Exception("Project 'path' not defined: {}".format(self.name))

		flow_paths = [os.path.join(self.path, flow) if not os.path.isabs(flow) else flow for flow in self.flows]

		self._flow_loader = FlowLoader(flow_paths)

		for uri, path in self._flow_loader.flow_files.items():
			self._log.debug("{0} : {1}".format(uri, path))

	def load_flow(self, flow_name, version=None):
		flow = self._flow_loader.load_from_canonical(flow_name, version=version)
		flow.project = self
		return flow

	def load_from_ref(self, ref):
		flow = self._flow_loader.load_from_ref(ref)
		flow.project = self
		return flow

	def get_conf(self, platform_name=None):
		conf = self.conf
		if platform_name is not None and platform_name in self._platforms:
			conf = conf.clone().merge(self._platforms[platform_name].conf)
		return conf

	def _repr(self, sb, indent=0):
		sb += ["Project [{}]".format(self.name)]
		sb += ["  path={}".format(self.path)]
		sb += ["  flows={}".format(self.flows)]
		sb += ["  conf={}".format(self.conf)]
		sb += ["  conf_rules={}".format(self.conf_rules)]
		for platform_name in sorted(self._platforms.keys()):
			self._platforms[platform_name]._repr(sb, 2)

	def __repr__(self):
		sb = []
		self._repr(sb)
		return "\n".join(sb)

class ProjectManager(object):

	FLOW_URI_RE = re.compile(r"^(?:([^\:]+)\:)?([^\/]+)(?:\/(.+))?$")

	def __init__(self, conf, base_path=None):

		self.conf = conf
		self.base_path = base_path or os.getcwd()

		self._log = woklogger.get_logger(_LOGGER_NAME)

		self._projects = {}

	def _iter_dict(self, conf):
		for name, pdesc in conf.items():
			yield name, pdesc

	def _iter_list(self, conf):
		for pdesc in conf:
			yield None, pdesc

	def initialize(self):
		self._log.info("Initializing projects ...")

		if Data.is_element(self.conf):
			iter_conf = self._iter_dict(self.conf)
		elif Data.is_list(self.conf):
			iter_conf = self._iter_list(self.conf)
		else:
			iter_conf = iter([])

		for name, pdesc in iter_conf:
			if isinstance(pdesc, basestring):
				pdesc = self._load_project_desc(pdesc, self.base_path)
			self._add_project_desc(pdesc, self.base_path)

		for name, project in sorted(self._projects.items(), key=lambda x: x[0]):
			project.initialize()

	def _load_project_desc(self, path, base_path=None):
		if not os.path.isabs(path):
			if base_path is not None:
				path = os.path.join(base_path, path)
			else:
				path = os.path.abspath(path)

		if not os.path.exists(path):
			raise Exception("Project path not found: {}".format(path))

		if os.path.isdir(path):
			path = os.path.join(path, "project.conf")

		if not os.path.isfile(path):
			raise Exception("Project configuration not found: {}".format(path))

		project = Data.element()
		cfg = ConfigFile(path)
		cfg.merge_into(project)
		if "path" not in project:
			project["path"] = os.path.dirname(path)

		if not os.path.isabs(project["path"]):
			project["path"] = os.path.join(os.path.dirname(path), project["path"])

		return project

	def _add_project_desc(self, pdesc, base_path=None):
		if "name" not in pdesc:
			raise Exception("Missing project name: {}".format(pdesc))

		name = pdesc["name"]

		if name not in self._projects:
			self._projects[name] = Project(pdesc, name=name, base_path=base_path)
		else:
			self._projects[name].merge(pdesc, base_path=base_path)

	def load_flow(self, uri):
		m = self.FLOW_URI_RE.match(uri)
		if m is None:
			raise Exception("Malformed flow uri: {}".format(uri))

		project_name, flow_name, version = m.groups()

		project = self._projects.get(project_name)
		if project is None:
			raise Exception("Project not found: {}".format(project_name))

		return project.load_flow(flow_name, version=version)

	def project_conf(self, project_name, platform_name=None):
		if project_name not in self._projects:
			return Data.element()

		project = self._projects[project_name]
		return project.get_conf(platform_name)

	def __iter__(self):
		return iter(self._projects.values())
