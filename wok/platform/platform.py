import os

from wok import logger
from wok.core.errors import ConfigMissingError

class Platform(object):
	"""
	Base class for platforms. A platform represents an execution environment where jobs get done. It can be in the
	same machine that the engine or not, it can be an iron cluster or a cloud cluster, and so on.

	Configuration namespace: **wok.platform**
	    **work_path**: working path for databases and state files.
	    **job_manager**: Job manager configuration
	        **type**: selected job manager. By default mcore.
	        **...**: job manager dependant configuration
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

	def _start(self):
		pass

	def start(self):
		"""
		Starts the use of the platform. Allows to start all the required services for the execution enviroment,
		and in the case of the cloud platforms the creation of the execution enviroment itself.
		:return:
		"""

		self._log.info("Starting '{}' platform ...".format(self._name))

		self._start()

		self._log.info("Platform '{}' started".format(self._name))

	def ports_manager(self):
		"""
		Returns the ports manager according to configuration
		:return: PortManager
		"""
		raise Exception("Unimplemented")

	def _close(self):
		pass

	def close(self):
		"""
		Ends the use of the platform. Close all the services not required anymore and destroy the instances
		in case of cloud platform.
		:return:
		"""

		self._log.info("Closing '{}' platform ...".format(self._name))

		self._close()

		self._log.info("Platform '{}' closed".format(self._name))