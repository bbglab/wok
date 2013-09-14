PORT_MODE_IN = "in"
PORT_MODE_OUT = "out"

class PortDataRef(object):
	def __init__(self, component_cname, port_name, partition=None, start=0, size=None, mode=PORT_MODE_OUT):

		self.component_cname = component_cname
		self.port_name = port_name

		self.partition_index = partition
		self.start = start
		self.size = size

		self.mode = mode

		self._last_partition = 0

	def __eq__(self, other):
		return self.component_cname == other.component_cname and self.port_name == other.port_name

	@property
	def refs(self):
		return [self]

	def link(self):
		return PortDataRef(self.component_cname, self.port_name, start=self.start, size=self.size, mode=PORT_MODE_IN)

	def slice(self, start, size):
		return PortDataRef(self.component_cname, self.port_name, start=start, size=size, mode=PORT_MODE_IN)

	def partition(self):
		part = self._last_partition
		self._last_partition += 1
		return PortDataRef(self.component_cname, self.port_name, partition=part, mode=PORT_MODE_OUT)

	def to_native(self):
		return dict(
			component=self.component_cname,
			port=self.port_name,
			partition=self.partition_index,
			start=self.start,
			size=self.size,
			mode=self.mode)

	def __repr__(self):
		sb = ["<", self.mode, " ", ".".join([self.component_cname, self.port_name])]
		if self.partition_index is not None or self.start != 0 or self.size is not None:
			sb += [" "]
			if self.partition_index is not None:
				sb + [str(self.partition_index), "."]
			sb += [str(self.start)]
			if self.size is not None:
				sb += [":", str(self.start + self.size - 1)]
		sb += [">"]
		return "".join(sb)

	@staticmethod
	def create(desc):
		if "refs" not in desc:
			return PortDataRef(
				desc["component"], desc["port"],
				partition=desc.get("partition"),
				start=desc.get("start"),
				size=desc.get("size"),
				mode=desc.get("mode"))
		else:
			return MergedPortDataRef(
				[PortDataRef.create(ref) for ref in desc["refs"]],
				start=desc.get("start"),
				size=desc.get("size"),
				mode=desc.get("mode"))

class MergedPortDataRef(object):
	def __init__(self, refs, start=0, size=None, mode=PORT_MODE_IN):
		self._refs = [ref.link() for ref in refs]

		self.start = start
		self.size = size

		self.mode = mode

	def __eq__(self, other):
		if not isinstance(other, MergedPortDataRef):
			return False

		for ref in self._refs:
			if ref != other:
				return False

		return True

	@property
	def refs(self):
		return self._refs

	def link(self):
		return MergedPortDataRef(self._refs, start=self.start, size=self.size, mode=PORT_MODE_IN)

	def slice(self, start, size):
		return MergedPortDataRef(self._refs, start=start, size=size, mode=PORT_MODE_IN)

	def to_native(self):
		return dict(
			refs=[ref.to_native() for ref in self._refs],
			start=self.start,
			size=self.size,
			mode=self.mode)

	def __repr__(self):
		return "[{}]".format(", ".join([repr(ref) for ref in self._refs]))
