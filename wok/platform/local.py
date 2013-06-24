from platform import Platform

class LocalPlatform(Platform):
	"""
	It represents an execution environment which share machine with the engine.
	"""

	def __init__(self, conf):
		Platform.__init__(self, "local", conf)

