from wok.data.port import Port

class MongoInPort(Port):

	index = 0

	def __init__(self, provider, name, coll, task_index):
		Port.__init__(self, provider, name)

		self._coll = coll

		self._task_index = task_index

	def send(self, data):
		self._coll.insert(dict(
			task=self._task_index,
			index=self.index,
			data=data))

		self.index += 1

	def size(self):
		return self._coll.count()


class MongoOutPort(Port):

	index = 0

	def __init__(self, provider, name, coll, task_index):
		Port.__init__(self, provider, name)

		self._coll = coll

		self._task_index = task_index

	def send(self, data):
		self._coll.insert(dict(
			task=self._task_index,
			index=self.index,
			data=data))

		self.index += 1

	def size(self):
		return self._coll.count()
