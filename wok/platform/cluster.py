import os
import subprocess
from urlparse import urlparse

from wok.core.cmd import ENV_PROJECT_PATH, ENV_SCRIPT_PATH, ENV_PLATFORM_SCRIPT_PATH
from wok.core.errors import MissingConfigParamError

from platform import Platform

class ClusterPlatform(Platform):
	"""
	It represents an execution environment in a remote cluster.

	Configuration:
	  **files_url**: The url for file transfers. Examples: ssh://host:22, file:///data
	  **remote_path**: The remote working path
	  **projects_path**: The path where projects files will be synced.
	  **rsync_path**: The path of the rsync binary. By default rsync.
	"""

	def __init__(self, conf):
		Platform.__init__(self, "cluster", conf)

		if "files_url" not in self._conf:
			raise MissingConfigParamError("files_url")

		url = urlparse(self._conf["files_url"])
		self._files_scheme = url.scheme
		self._files_host = url.netloc

		if self._files_scheme not in ["ssh", "file", "rsync"]:
			raise Exception("Unsupported scheme: {}".format(self._files_scheme))

		self._remote_path = self._conf.get("remote_path", self._work_path)
		self._projects_path = self._conf.get("projects_path", os.path.join(self._work_path, "projects"))

		self._rsync_path = self._conf.get("rsync_path", "rsync")

	def _start(self):
		# TODO create remote_path if it doesn't exist
		# TODO rsync needed files ?
		pass

	def sync_project(self, project):
		self._log.info("Synchronizing project {} ...".format(project.name))

		if self._files_scheme in ["file", ""]:
			cmd = "mkdir -p {}".format(self._projects_path)
		elif self._files_scheme in ["ssh", "rsync"]:
			cmd = "ssh {} 'mkdir -p {}'".format(self._files_host, self._projects_path)

		ret = subprocess.call(cmd, shell=True)
		if ret != 0:
			self._log.error("Error while creating destination path for project {}:\n{}".format(project.name, cmd))

		cmd = [self._rsync_path, "-aq"]
		cmd += ["{}/".format(os.path.normpath(project.path))]
		if self._files_scheme in ["file", ""]:
			cmd += ["{}/{}".format(self._files_host, self._projects_path, project.name)]
		elif self._files_scheme == "ssh":
			cmd += ["{}:{}/{}".format(self._files_host, self._projects_path, project.name)]
		elif self._files_scheme == "rsync":
			cmd += ["{}::{}/{}".format(self._files_host, self._projects_path, project.name)]

		ret = subprocess.call(" ".join(cmd), shell=True)
		if ret != 0:
			self._log.error("Error syncing project {}:\n{}".format(project.name, " ".join(cmd)))

	"""
	def _filter_job_submissions(self, job_submissions):
		for js in job_submissions:
			project = js.case.project
			flow_path = os.path.dirname(js.task.flow_path)
			rel_path = os.path.relpath(flow_path, project.path)
			remote_flow_path = os.path.join(self._projects_path, project.name, rel_path)
			js.env[ENV_FLOW_PATH] = remote_flow_path
			js.env[ENV_PLATFORM_SCRIPT_PATH] = os.path.join(remote_flow_path, js.env[ENV_SCRIPT_PATH])
			yield js
	"""
