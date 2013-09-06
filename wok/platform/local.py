from platform import Platform

class LocalPlatform(Platform):
	"""
	It represents an execution environment in the same machine than the engine.
	"""

	def __init__(self, conf):
		Platform.__init__(self, "local", conf)

