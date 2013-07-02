class Partition(object):
	def __init__(self, stream, start, size):
		self._stream = stream
		self.start = start
		self.size = size