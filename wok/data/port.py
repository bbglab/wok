
class Port(object):
	def __init__(self, provider, name):
		self._provider = provider
		self.name = name

class PortDataRef(object):
	def __init__(self, conf):
		self.conf = conf

class SinglePortDataRef(PortDataRef):
	def __init__(self, module_path, port_name, **kwargs):
		PortDataRef.__init__(self, kwargs)

		self.module_path = module_path
		self.port_name = port_name

	def __eq__(self, other):
		return self.module_path == other.module_path and self.port_name == other.port_name

	def __repr__(self):
		return "<{}>".format(".".join([self.module_path, self.port_name]))

class MergedPortDataRef(PortDataRef):
	def __init__(self, refs, **kwargs):
		PortDataRef.__init__(self, kwargs)

		self.refs = refs

	def __eq__(self, other):
		for ref in refs:
			if ref != other:
				return False
		return True

	def __repr__(self):
		return "[{}]".format(", ".join([repr(ref) for ref in self.refs]))