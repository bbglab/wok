import os
import re

from wok import logger
from wok.config.data import Data
from wok.core.flow.loader import FlowLoader

_LOGGER_NAME = "wok.projects"

class ProjectManager(object):

	FLOW_URI_RE = re.compile(r"^(?:([^\:]+)\:)?([^\/]+)(?:\/(.+))?$")

	def __init__(self, conf, base_path=None):

		self.conf = conf
		self.base_path = base_path or os.getcwd()

		self._log = logger.get_logger(_LOGGER_NAME)

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
			if name is None:
				name = pdesc["name"]
			self._projects[name] = Project(pdesc, name=name)

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

		return project

	def load_flow(self, uri):
		m = self.FLOW_URI_RE.match(uri)
		if m is None:
			raise Exception("Malformed flow uri: {}".format(uri))

		project_name, flow_name, version = m.groups()

		project = self._projects.get(project_name)
		if project is None:
			raise Exception("Project not found: {}".format(project_name))

		return project.load_flow(flow_name, version=version)

	def __iter__(self):
		return iter(self._projects.values())

class Project(object):
	def __init__(self, conf, name=None, path=None):
		self._log = logger.get_logger(_LOGGER_NAME)

		self.name = name or conf.get("name")
		self.path = path or conf.get("path")
		self._source = conf.get("source")
		self._setup = conf.get("setup")
		self._update = conf.get("update")
		self._clean = conf.get("clean")
		self.flows = conf.get("flows")
		self.platforms = conf.get("platforms") # + prefix = allowed, - prefix = not allowed

		self._flow_loader = None

	def initialize(self):
		self._log.info("Initializing project {} ...".format(self.name))

		if not os.path.exists(self.path):
			self.download()
			self.setup()

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

	def download(self):
		# TODO Download
		if not os.path.exists(self.path):
			raise Exception("Failed downloading of project {}".format(self.name))

	def setup(self):
		pass

	def update(self):
		pass

	def clean(self):
		pass

	def load_flows(self):
		pass

	def __repr__(self):
		sb = ["Project:"]
		sb += ["  name={0}".format(self.name)]
		sb += ["  path={0}".format(self.path)]
		sb += ["  flows={0}".format(self.flows)]
		return "\n".join(sb)