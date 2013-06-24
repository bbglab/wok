from platform import Platform

class ClusterPlatform(Platform):
	"""
	It represents an execution environment in a remote cluster.

	Configuration:
	  **remote_path**: The remote working path
	"""

	def __init__(self, conf):
		Platform.__init__(self, "cluster", conf)

		self._remote_path = self._conf.get("remote_path", self._work_path)

	def _start(self):
		# TODO create remote_path if it doesn't exist
		# TODO rsync needed files ?
		pass
