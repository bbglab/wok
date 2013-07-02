import os
import re

from wok import logger
from wok.config.data import Data
from wok.core.flow.loader import FlowLoader

_LOGGER_NAME = "wok.projects"

class ProjectManager(object):

	FLOW_URI_RE = re.compile(r"^(?:([^\:]+)\:)?([^\/]+)(?:\/(.+))?$")

	def __init__(self, conf, log_conf=None):

		self._log = logger.get_logger(_LOGGER_NAME, conf=log_conf)

		self._projects = {}

		if Data.is_element(conf):
			self._init_from_dict(conf)
		elif Data.is_list(conf):
			self._init_from_list(conf)
		#else:
		#	raise

	def _init_from_dict(self, conf):
		for name, pdesc in conf.items():
			self._projects[name] = Project(pdesc, name=name)

	def _init_from_list(self, conf):
		for pdesc in conf:
			name = pdesc["name"]
			self._projects[name] = Project(pdesc, name=name)

	def initialize(self):
		self._log.info("Initializing projects ...")
		for name, project in sorted(self._projects.items(), key=lambda x: x[0]):
			project.initialize()

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

		flow_paths = [os.path.join(self.path, flow) for flow in self.flows]
		self._flow_loader = FlowLoader(flow_paths)

	def load_flow(self, flow_name, version=None):
		flow = self._flow_loader.load_from_canonical(flow_name, version=version)
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