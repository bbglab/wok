import sqlite3
import os
import json

from wok.data.port import Port
from wok.core.utils.io import FileLockEx

class SourceData(object):
	def __init__(self, path):
		self._path = path

		self._db = None

	def _create(self):
		self._db.executescript("""
		CREATE TABLE IF NOT EXISTS elements (
			workitem	INTEGER,
			'index'		INTEGER,
			data		TEXT,

			PRIMARY KEY (workitem, 'index')
		);
		""")

	def _connection(self):
		if self._db is None:
			self._db = sqlite3.connect(self._path, timeout=1800)
			with FileLockEx("{}.lock".format(self._path), "r+") as f:
				if f.read() != "1":
					self._create()
					f.seek(0)
					f.write("1")
					f.truncate()

		return self._db

	def count(self):
		con = self._connection()
		c = con.execute("SELECT COUNT(*) FROM elements")
		return c.fetchone()[0]

	def find(self, limit=None, skip=0):
		con = self._connection()
		c = con.cursor()
		sql = ["SELECT * FROM elements"]

		# WARNING sqlite does not allow to use OFFSET without LIMIT
		assert limit is not None or (limit is None and skip == 0)

		if limit is not None:
			sql += ["LIMIT", str(limit)]
			if skip > 0:
				sql += ["OFFSET", str(skip)]

		c.execute(" ".join(sql))

		return c

	def insert(self, e):
		con = self._connection()

		data = json.dumps(e["data"])

		con.execute("INSERT INTO elements (workitem, 'index', data) VALUES (?, ?, ?)",
					(e["workitem"], e["index"], data))

		con.commit()

	def __repr__(self):
		return "SourceData('{}')".format(self._path)

class SourceCursor(object): # not used
	def __init__(self, cursor):
		self._cursor_iter = iter(cursor)

	def next(self):
		e = self._cursor_iter.next()
		return dict(
			workitem=e[0],
			index=e[1],
			data=json.loads(e[2]))

class FilesInPort(Port):

	def __init__(self, provider, sources, start, size):
		Port.__init__(self, provider)

		self._sources = sources

		self._start = start
		self._size = size

		self._src_index = 0
		self._src_skip = 0
		self._src_limit = 0
		self._cursor = None

	def __iter__(self):
		return self

	def _open(self):
		if not self._sources:
			raise StopIteration()

		index = 0
		start = self._start
		src = self._sources[0]
		src_size = src.count()

		while start >= src_size and index < len(self._sources):
			index += 1
			start -= src_size
			src = self._sources[index]
			src_size = src.count()

		assert index < len(self._sources)

		self._src_index = index
		self._src_skip = start
		self._src_limit = min(src_size - start, self._size)

		self._find()

	def _find(self):
		kwargs = dict()
		if self._src_skip > 0:
			kwargs["skip"] = self._src_skip
		if self._src_limit is not None:
			kwargs["limit"] = self._src_limit
		src = self._sources[self._src_index]
		self._cursor = src.find(**kwargs)

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
				msg = self._cursor.fetchone()
				if msg is None:
					raise StopIteration()
				msg = json.loads(msg[2])
				self._size -= 1
			except StopIteration:
				self._src_index += 1

				if self._src_index == len(self._sources):
					raise

				src = self._sources[self._src_index]
				src_size = src.count()
				self._src_skip = 0
				self._src_limit = min(src_size, self._size)

				self._find()

		return msg

	def size(self):
		total_size = 0
		for src in self._sources:
			total_size += src.count()
		return total_size # TODO cache ?

	def close(self):
		pass

	def __repr__(self):
		return "source={} skip={} limit={} sources={}".format(self._src_index, self._src_skip, self._src_limit, repr(self._sources))


class FilesOutPort(Port):

	def __init__(self, provider, source, workitem_index):
		Port.__init__(self, provider)

		self._source = source

		self._workitem_index = workitem_index

		self._next_index = 0

	def send(self, data):
		self._source.insert(dict(
			workitem=self._workitem_index,
			index=self._next_index,
			data=data))
		self._next_index += 1

	def size(self):
		return self._source.count()

	def close(self):
		pass
