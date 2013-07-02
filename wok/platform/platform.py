import os

from wok import logger
from wok.config import Data
from wok.core import events
from wok.core.errors import ConfigMissingError
from wok.core.callback import CallbackManager
from wok.core.cmd import create_command_builder
from wok.jobs import create_job_manager, JobSubmission
from wok.data import create_data_provider

class Platform(object):
	"""
	Base class for platforms. A platform represents an execution environment where jobs get done. It can be in the
	same machine that the engine or not, it can be an iron cluster or a cloud cluster, and so on.

	Configuration namespace: **wok.platform**
	    **work_path**: working path for databases and state files.
	    **jobs**: Job manager configuration
	        **type**: selected job manager. By default mcore.
	        **...**: job manager configuration
	    **data**: Data provider configuration
	        **type**: selected data provider. By default mongo.
	        **...**: data provider configuration
	    **log**: logging configuration
	"""

	def __init__(self, name, conf):
		self._name = name
		self._conf = conf

		self._log = logger.get_logger(name="wok.platform.{}".format(name), conf=conf.get("log"))

		if "work_path" not in self._conf:
			raise ConfigMissingError("work_path")

		self._work_path = conf["work_path"]
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		self._job_manager = self._create_job_manager()

		self._data = self._create_data_provider()

		self._callbacks = CallbackManager(delegates=[
			(events.JOB_UPDATE, self._job_manager.callbacks)])

	def _create_job_manager(self):
		"""
		Create a job manager according to the configuration
		:return: JobManager
		"""

		name = "mcore"
		conf = Data.element()
		if "jobs" in self._conf:
			conf.merge(self._conf["jobs"])
			if "type" in conf:
				name = conf["type"]

		if "work_path" not in conf:
			conf["work_path"] = os.path.join(self._work_path, "jobs", name)

		self._log.info("Creating '{}' job manager ...".format(name))
		self._log.debug("Job manager configuration: {}".format(repr(conf)))

		return create_job_manager(name, conf)

	def _create_data_provider(self):
		"""
		Create a data provider
		:return: DataProvider
		"""

		conf = self._conf.get("data", default=Data.element)
		name = conf.get("type", "mongo")

		self._log.info("Creating '{}' data provider ...".format(name))
		self._log.debug("Data provider configuration: {}".format(repr(conf)))

		return create_data_provider(name, conf)

	def _create_job_submission(self, task):

		js = JobSubmission(
			task=task, instance_id=task.parent.instance.name, task_id=task.id, task_conf=task.conf,
			priority=max(min(task.priority, 1), 0))

		execution = task.parent.execution
		cmd_builder = create_command_builder(execution.mode)

		js.script, js.env = cmd_builder.prepare(task)

		return js

	def _job_submissions(self, tasks):
		for task in tasks:
			yield self._create_job_submission(task)

	def _start(self):
		pass

	def _close(self):
		pass

	# ------------------------------------------------------------------------------------------------------------------

	@property
	def callbacks(self):
		return self._callbacks

	@property
	def jobs(self):
		return self._job_manager

	@property
	def data(self):
		return self._data

	def start(self):
		"""
		Starts the use of the platform. Allows to start all the required services for the execution enviroment,
		and in the case of the cloud platforms the creation of the execution enviroment itself.
		:return:
		"""

		self._log.info("Starting '{}' platform ...".format(self._name))

		if self._data is not None:
			self._data.start()

		if self._job_manager is not None:
			self._job_manager.start()

		self._start()

		self._log.info("Platform '{}' started".format(self._name))

	def submit(self, tasks):
		"""
		Submit tasks. Yields tuples (JobSubmission, job id) for each task.
		:param tasks: list or iterator of tasks
		"""
		submissions = self._job_submissions(tasks)
		for js, job_id in self._job_manager.submit(submissions):
			yield js, job_id

	def sync_project(self, project):
		"""
		Synchronize the project files with the platform node/s
		:param project:
		"""
		pass

	def close(self):
		"""
		Ends the use of the platform. Close all the services not required anymore and destroy the instances
		in case of cloud platform.
		:return:
		"""

		self._log.info("Closing '{}' platform ...".format(self._name))

		self._close()

		if self._job_manager is not None:
			self._job_manager.close()

		if self._data is not None:
			self._data.close()

		self._log.info("Platform '{}' closed".format(self._name))