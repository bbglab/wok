from wok.data.port import Port

class MongoInPort(Port):

	def __init__(self, provider, name, coll, start, size):
		Port.__init__(self, provider, name)

		self._coll = coll
		self._start = start
		self._size = size

		self._cursor = None

	def __iter__(self):
		return self

	def next(self):
		return self.read()

	def read(self):
		if self._cursor is None:
			kwargs = dict()
			if self._start > 0:
				kwargs["skip"] = self._start
			if self._size is not None and self._size > 0:
				kwargs["limit"] = self._size
			self._cursor = self._coll.find(**kwargs)

		msg = self._cursor.next()
		return msg["data"] if "data" in msg else None

	def size(self):
		return self._coll.count()

	def close(self):
		pass


class MongoOutPort(Port):

	indices = {}

	def __init__(self, provider, name, coll, task_index):
		Port.__init__(self, provider, name)

		self._coll = coll

		self._task_index = task_index

	def __next_index(self):
		if self.name not in self.indices:
			self.indices[self.name] = 0
		next_index = self.indices[self.name]
		self.indices[self.name] += 1
		return next_index

	def send(self, data):
		self._coll.insert(dict(
			task=self._task_index,
			index=self.__next_index(),
			data=data))

	def size(self):
		return self._coll.count()

	def close(self):
		pass
