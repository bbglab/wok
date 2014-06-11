import os

from wok import logger
from wok.config import Data
from wok.core import events
from wok.core.errors import MissingConfigParamError
from wok.core.callback import CallbackManager
from wok.jobs import create_job_manager
from wok.data import data_provider_factory
from wok.storage import storage_factory

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
		self._conf = conf
		self._name = conf.get("name", name)

		self._log = logger.get_logger("wok.platform.{}".format(name))

		if "work_path" not in self._conf:
			raise MissingConfigParamError("work_path")

		self._work_path = conf["work_path"]
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		self._data = self._create_data_provider()

		self._storage = self._create_storage()

		self._job_manager = self._create_job_manager()

		self._callbacks = CallbackManager(delegates=[
			(events.JOB_UPDATE, self._job_manager.callbacks)])

	def _create_data_provider(self):
		"""
		Create a data provider according to the configuration.
		:return: DataProvider
		"""

		conf = self._conf.get("data", default=Data.element)
		name = conf.get("type", "files")

		if name == "files" and "path" not in conf:
			conf["path"] = os.path.join(self._work_path, "data_{}".format(name))

		return data_provider_factory.create(conf, type=name, logger=self._log)

	def _create_storage(self):
		"""
		Create a storage according to the configuration.
		"""

		conf = self._conf.get("storage", default=Data.element)
		name = conf.get("type", "files")

		if name == "files" and "path" not in conf:
			conf["path"] = os.path.join(self._work_path, "storage")

		return storage_factory.create(conf, type=name, logger=self._log)

	def _create_job_manager(self):
		"""
		Create a job manager according to the configuration.
		:return: JobManager
		"""

		conf = self._conf.get("jobs", default=Data.element)
		name = conf.get("type", "mcore")

		if "work_path" not in conf:
			conf["work_path"] = os.path.join(self._work_path, "jobs_{}".format(name))

		self._log.info("Creating '{}' job manager ...".format(name))
		self._log.debug("Job manager configuration: {}".format(repr(conf)))

		return create_job_manager(name, conf)

	def _filter_job_submissions(self, job_submissions):
		for js in job_submissions:
			yield js

	def _start(self):
		pass

	def _close(self):
		pass

	# ------------------------------------------------------------------------------------------------------------------

	@property
	def name(self):
		return self._name

	@property
	def conf(self):
		return self._conf

	@property
	def callbacks(self):
		return self._callbacks

	@property
	def data(self):
		return self._data

	@property
	def storage(self):
		return self._storage

	@property
	def jobs(self):
		return self._job_manager

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

	def submit(self, job_submissions):
		"""
		Submit job submissions to the job manager.
		:param job job_submissions: iterator of JobSubmission
		"""
		filtered_submissions = self._filter_job_submissions(job_submissions)
		for submit_data in self._job_manager.submit(filtered_submissions):
			yield submit_data

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