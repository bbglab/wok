import os
import struct

from wok.portio import PortData, DataReader

TYPE_PATH_DATA = "path_data"

class PathData(PortData):
	def __init__(self, path = None, partition = -1, start = 0, size = -1, conf = None):
		if conf is not None:
			self._path = conf.get("path", path)
			self._partition = conf.get("partition", partition, dtype=int)
			self._start = conf.get("start", start, dtype=int)
			self._size = conf.get("size", size, dtype=int)
		else:
			self._path = path
			self._partition = partition
			self._start = start
			self._size = size

		self.first_partition = 0
		self.last_partition = 0

	def fill_element(self, e):
		e["type"] = TYPE_PATH_DATA
		e["path"] = self._path
		e["partition"] = self._partition
		e["start"] = self._start
		e["size"] = self._size
		return e

	def _partition_size(self, partition):
		path = os.path.join(self._path, "%06i.index" % partition)
		if os.path.exists(path):
			return os.path.getsize(path) / 8
		else:
			return 0

	def get_slice(self, partition = None, start = None, size = None):
		if start is None and size is None:
			if partition is not None:
				return PathData(self._path, partition)
			else:
				return PathData(self._path, self._partition)
		
		if partition is None:
			partition = 0

		ps = self._partition_size(partition)
		
		if start is None:
			start = 0
		else:
			while start > ps:
				partition += 1
				start -= ps
				ps = self._partition_size(partition)

		if size is None:
			size = 0

		return PathData(self._path, partition, start, size)
		
		"""
		sz = min(ps - start, size)
		partitions = [PathData(self._path, partition, start, sz)]

		size -= sz
		while size > 0:
			partition += 1
			ps = self._partition_size[partition]
			sz = min(ps, size)
			partitions += [PathData(self._path, partition, 0, sz)]
			size -= sz

		if len(partitions) == 1:
			return partitions[0]
		else:
			return MultiData(partitions)
		"""
	"""
	def _calc_partition_sizes(self):
		self._partition_size = []
		if os.path.exists(self._path):
			for f in os.listdir(self._path):
				if f.endswith(".index"):
					p = int(f[:-6])
					s = len(self._partition_size)
					if p >= s:
						self._partition_size += [0] * (p - s + 1)
					path = os.path.join(self._path, f)
					self._partition_size[p] = os.path.getsize(path) / 8
	"""
	
	def size(self):
		if not os.path.exists(self._path):
			self._size = 0
		elif self._partition == -1:
			self._size = 0
			if os.path.exists(self._path):
				for f in os.listdir(self._path):
					if f.endswith(".index"):
						path = os.path.join(self._path, f)
						self._size += os.path.getsize(path) / 8
		else:
			path = os.path.join(self._path, "%06i.index" % self._partition)
			if os.path.exists(path):
				self._size = (os.path.getsize(path) / 8) - self._start
			else:
				self._size = 0
		return self._size

	def reader(self):
		if self._partition == -1 or self._size == -1:
			raise Exception("A reader can not be created without knowing the partition and/or size")
	
		return PartitionDataReader(self._path, self._partition, self._start, self._size)
	
	def writer(self):
		if self._partition == -1:
			raise Exception("A writer can not be created without knowing the partition")

		return PartitionDataWriter(self._path, self._partition)
		
	def __repr__(self):
		sb = [self._path]
		if self._partition != -1 or self._start != 0 or self._size != -1:
			sb += [" "]
			if self._partition != -1:
				sb + ["%i." % self._partition]
			sb += ["%i" % self._start]
			if self._size != -1:
				sb += [":%i" % (self._start + self._size - 1)]
		return "".join(sb)

class PartitionDataReader(DataReader):
	def __init__(self, path, partition, start, size):
		self._path = path
		self._partition = partition
		self._start = start
		self._size = size
	
		self._index_path = None
		self._data_path = None
		
		self._index_f = None
		self._data_f = None

	def _open(self):
		if self._data_f is not None:
			self.close()

		self._index_path = os.path.join(self._path, "%06i.index" % self._partition)
		self._data_path = os.path.join(self._path, "%06i.data" % self._partition)
		
		self._index_f = open(self._index_path, "rb")
		self._data_f = open(self._data_path, "r")
		
		self._index_f.seek(self._start * 8)
	
	def close(self):
		if self._index_f is not None:
			self._index_f.close()
			self._index_f = None
		if self._data_f is not None:
			self._data_f.close()
			self._data_f = None

	def is_opened(self):
		return self._data_f is not None

	def next(self):
		if self._size == 0:
			raise StopIteration()

		if self._data_f is None:
			self._open()

		d = self._index_f.read(8)
		if len(d) < 8:
			self._partition += 1
			self._start = 0

			self._open()
			
			d = self._index_f.read(8)
			
			if len(d) < 8:
				raise StopIteration()

		pos = struct.unpack("Q", d)[0]
		self._data_f.seek(pos)
		data = self._data_f.readline().rstrip()
		
		self._size -= 1

		return data
	
class PartitionDataWriter(object):
	def __init__(self, path, partition):
		self._path = path
		self._partition = partition

		if not os.path.exists(path):
			os.makedirs(path)
		
		self._index_path = os.path.join(path, "%06i.index" % partition)
		self._data_path = os.path.join(path, "%06i.data" % partition)

		self._index_f = None
		self._data_f = None

	def _open(self):
		if self._data_f is not None:
			self.close()
		self._index_f = open(self._index_path, "wb+")
		self._data_f = open(self._data_path, "w+")
		
	def close(self):
		if self._index_f is not None:
			self._index_f.close()
			self._index_f = None
		if self._data_f is not None:
			self._data_f.close()
			self._data_f = None
		
	def write(self, data):
		if self._data_f is None:
			self._open()

		if not isinstance(data, list):
			data = [data]
		
		for d in data:
			# Update the index
			pos = self._data_f.tell()
			self._index_f.write(struct.pack("Q", pos))
		
			# Write the data
			# TODO encode \n from data
			self._data_f.write(d.replace("\n", r"\n"))
			self._data_f.write("\n")

	def __repr__(self):
		return "%s/%06i" % (self._path, self._partition)
