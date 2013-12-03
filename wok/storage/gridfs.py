import os
import shutil
import time
import tempfile
from StringIO import StringIO

import pymongo
gridfs = __import__("gridfs")

from wok.storage.storage import Storage, StorageContainer, StorageObject, StorageError, NotEmptyContainerDeletedError

class GridFsStorage(Storage):

	plugin_type = "gridfs"

	plugin_required_conf = ["url"]

	def __init__(self, conf):
		super(GridFsStorage, self).__init__(conf)

		self._url = conf["url"]

		database = conf.get("database")

		self._client = pymongo.MongoClient(self._url)
		if isinstance(self._client, pymongo.database.Database):
			self._db = self._client
			if database is not None:
				raise StorageError("Database already specified in the url")
		else:
			self._db = self._client[database or "wok"]

	def create_container(self, name):
		"""
		Creates a new container. If a container with this name already exists then it just returns it.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""

		return GridFsContainer(self, name)

	def get_container(self, name):
		"""
		Returns a container. If no container exists then a new one will be created.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""

		return GridFsContainer(self, name)

	def delete_container(self, name, delete_objects=True):
		"""
		Deletes a container. Nothing happens if it doesn't exist. Fails if it contains objects unless delete_objects is True.
		:param name: the name of the container. It can not contain "/".
		:param delete_objects: If the container has objects then delete them before.
		"""

		chunks_coll_name = "{}.chunks".format(name)
		if chunks_coll_name in self._db.collection_names():
			if delete_objects:
				self._db.drop_collection(chunks_coll_name)
				files_coll_name = "{}.files".format(name)
				self._db.drop_collection(files_coll_name)
			else:
				raise NotEmptyContainerDeletedError(name)

	def exists_container(self, name):
		"""
		Check if the container exists.
		:param name: a container name.
		:return: True or False
		"""

		chunks_coll_name = "{}.chunks".format(name)
		return chunks_coll_name in self._db.collection_names()

	def list_containers(self):
		"""
		Iterator for listing containers within this storage.
		"""

		for coll_name in self._db.collection_names():
			if coll_name.endswith(".chunks"):
				yield coll_name

class GridFsContainer(StorageContainer):

	def __init__(self, storage, name):
		super(GridFsContainer, self).__init__(storage, name)

		self._gridfs = gridfs.GridFS(storage._db, name)

	def create_object(self, name, **metadata):
		"""
		Create a new object. If an object with this name already exists then just returns it.
		:param name: an object name.
		:return: the object.
		"""

		return GridFsObject(self, name, **metadata)

	def get_object(self, name):
		"""
		Return an object by name.
		:param name: the object's name.
		:return: the object.
		"""

		return GridFsObject(self, name)

	def delete_object(self, name):
		"""
		Delete an object.
		:param name: an object name.
		"""

		GridFsObject(self, name).delete()

	def exists_object(self, name):
		"""
		Check if the object exists.
		:param name: an object name.
		:return: True or False
		"""

		return GridFsObject(self, name).exists()

	def list_objects(self, prefix=None, delimiter=None):
		"""
		Iterator for listing objects within this container.
		:param prefix: list only the objects which name starts with this prefix. Useful for pseudo-hierarchical navigation.
		:param delimiter: list distinct object names up to either the first delimiter or the end. Useful for pseudo-hierarchical navigation.
		"""

		listed = set()
		for name in self._gridfs.list():
			if prefix is not None:
				if not name.startswith(prefix):
					continue
				name = name[len(prefix):]
			if delimiter is not None:
				pos = name.find(delimiter)
				if pos >= 0:
					name = name[:pos + 1]
					if name in listed:
						continue
					listed.add(name)
			yield name

class GridFsObject(StorageObject):

	_BUF_SIZE = 4 * 1024 * 1024 # 4M

	def __init__(self, container, name, **metadata):
		super(GridFsObject, self).__init__(container, name, **metadata)

		self._gridfs = container._gridfs

	def put_data(self, src, overwrite=True, check_timestamp=True):
		"""
		Put data into the object from a source.

		:param src: It can be the path to a file to get data from or a file-like object.
		:param overwrite: whether to overwrite the object if it already exists or fail.
		"""

		if isinstance(src, basestring):
			src_timestamp = os.path.getmtime(src)
			self._metadata["__timestamp__"] = src_timestamp
			self._metadata["__filename__"] = src_filename = src
			src = open(src, "r")
			close_src = True
		else:
			src_filename = None
			src_timestamp = None
			close_src = False

		update = True
		if self._gridfs.exists(self._name):
			if not overwrite:
				raise StorageObjectAlreadyExists(repr(self))

			if check_timestamp:
				obj = self._gridfs.get(self._name)
				metadata = obj.metadata or {}
				dst_timestamp = metadata.get("__timestamp__")
				#dst_filename = obj.filename
				dst_filename = metadata.get("__filename__")
				update = src_filename is None or dst_filename is None or src_timestamp is None or dst_timestamp is None\
							or src_filename != dst_filename or src_timestamp != dst_timestamp

		if update:
			self._gridfs.delete(self._name)

			dest = self._gridfs.new_file(_id=self._name, filename=self._name, metadata=self._metadata)

			buf = src.read(self._BUF_SIZE)
			while len(buf) > 0:
				dest.write(buf)
				buf = src.read(self._BUF_SIZE)

			dest.close()

		if close_src:
			src.close()

	def get_data(self, dst=None, check_timestamp=True):
		"""
		Get the object data into the dest file.
		:param dst: Where to save the data. It can be an string with the destination path, a file-like object or None.
		             If it is None a temporary file will be created if the file does not exists already in the local file system.
		:return: The destination path when dest is not a file-like object.
		"""

		if self._gridfs.exists(self._name):
			src = self._gridfs.get(self._name)
			src_timestamp = src.metadata.get("__timestamp__") if src.metadata is not None else None
		else:
			src = StringIO("")
			src_timestamp = None

		update = True
		if isinstance(dst, basestring):
			dst_filename = dst
			dst_timestamp = os.path.getmtime(dst) if os.path.exists(dst) else None
			update = not check_timestamp or src_timestamp is None or dst_timestamp is None\
						or src_timestamp != dst_timestamp
			if update:
				dst = open(dst, "w")
			close_dest = update
		elif dst is None:
			dst = tempfile.NamedTemporaryFile(delete=False)
			dst_filename = dst.name
			close_dest = True
		else:
			dst_filename = None
			close_dest = False

		if update:
			buf = src.read(self._BUF_SIZE)
			while len(buf) > 0:
				dst.write(buf)
				buf = src.read(self._BUF_SIZE)
			src.close()

		if close_dest:
			dst.close()

		return dst_filename

	def get_metadata(self):
		"""
		Get the object metadata.
		:return a dictionary with the metadata
		"""

		return dict(self._gridfs.get(self._name).metadata)

	def exists(self):
		"""
		Check whether the object exists.
		:return True or False
		"""
		return self._gridfs.exists(self._name)

	def delete(self):
		"""
		Delete the object from its container.
		"""
		self._gridfs.delete(self._name)