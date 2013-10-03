from wok.data.port import Port

class MongoInPort(Port):

	def __init__(self, provider, collections, start, size):
		Port.__init__(self, provider)

		self._collections = collections

		self._start = start
		self._size = size

		self._coll_index = 0
		self._coll_skip = 0
		self._coll_limit = 0
		self._cursor = None

	def __iter__(self):
		return self
	
	def _open(self):
		if not self._collections:
			raise StopIteration()

		index = 0
		start = self._start
		coll = self._collections[0]
		coll_size = coll.count()

		while start >= coll_size and index < len(self._collections):
			index += 1
			start -= coll_size
			coll = self._collections[index]
			coll_size = coll.count()
			
		self._coll_index = index
		self._coll_skip = start
		self._coll_limit = min(coll_size - start, self._size)

		self._find()

	def _find(self):
		kwargs = dict()
		if self._coll_skip > 0:
			kwargs["skip"] = self._coll_skip
		if self._coll_limit is not None and self._coll_limit > 0:
			kwargs["limit"] = self._coll_limit
		coll = self._collections[self._coll_index]
		self._cursor = coll.find(**kwargs)

	def next(self):
		return self.read()

	def read(self):
		if self._size == 0:
			raise StopIteration()

		if self._cursor is None:
			self._open()

		msg = None
		while msg is None:
			try:
				msg = self._cursor.next()
				self._size -= 1
			except StopIteration:
				self._coll_index += 1

				if self._coll_index == len(self._collections):
					raise

				coll = self._collections[self._coll_index]
				coll_size = coll.count()
				self._coll_skip = 0
				self._coll_limit = min(coll_size, self._size)

				self._find()

		return msg["data"] if "data" in msg else None

	def size(self):
		total_size = 0
		for coll in self._collections:
			total_size += coll.count()
		return total_size # TODO cache ?

	def close(self):
		pass

	def __repr__(self):
		return "{} {}".format(self._coll_index, repr(self._collections))


class MongoOutPort(Port):

	def __init__(self, provider, coll, workitem_index):
		Port.__init__(self, provider)

		self._coll = coll

		self._workitem_index = workitem_index

		self._next_index = 0

	def send(self, data):
		self._coll.insert(dict(
			workitem=self._workitem_index,
			index=self._next_index,
			data=data))
		self._next_index += 1

	def size(self):
		return self._coll.count()

	def close(self):
		pass
