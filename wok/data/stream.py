class Stream(object):
	"""
	Represents an stream of data elements.
	"""

	JOIN_DOT_PRODUCT = "dot"

	def __init__(self, provider, module=None, desc=None):
		self._provider = provider

		if desc is not None:
			self._name = desc["name"]
			self._join = desc["join"]
		#TODO elif module is not None:

		self._ports = []

	def add_port_data(self, port_data):
		self._ports += [port_data]