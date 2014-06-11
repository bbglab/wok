import os
import shutil
import time
import json
from StringIO import StringIO

from wok.core.errors import MissingRequiredArgumentError
from wok.storage.storage import Storage, StorageContainer, StorageObject, StorageError, NotEmptyContainerDeletedError

def _normalize_container_name(name):
	return name.replace("..", "__").replace("/", "::")

def _ensure_path_exists(path):
	try:
		os.makedirs(path)
	except Exception as ex:
		time.sleep(0.5)
		if not os.path.exists(path):
			raise ex

class FilesStorage(Storage):

	plugin_type = "files"

	plugin_required_conf = ["path"]

	def __init__(self, conf):
		super(FilesStorage, self).__init__(conf)

		self._path = conf["path"]

	@property
	def path(self):
		return self._path

	def create_container(self, name):
		"""
		Creates a new container. If a container with this name already exists then it just returns it.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""

		nname = _normalize_container_name(name)
		cpath = os.path.join(self._path, nname)

		if os.path.exists(cpath) and not os.path.isdir(cpath):
			raise StorageError("Expecting a folder at {}".format(cpath))

		return FilesStorageContainer(self, nname)

	def get_container(self, name):
		"""
		Returns a container. If no container exists then a new one will be created.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""

		nname = _normalize_container_name(name)
		return FilesStorageContainer(self, nname)

	def delete_container(self, name, delete_objects=True):
		"""
		Deletes a container. Nothing happens if it doesn't exist. Fails if it contains objects unless delete_objects is True.
		:param name: the name of the container. It can not contain "/".
		:param delete_objects: If the container has objects then delete them before.
		"""

		nname = _normalize_container_name(name)
		cpath = os.path.join(self._path, nname)

		if os.path.exists(cpath):
			if delete_objects:
				shutil.rmtree(cpath)
			elif len(os.listdir(cpath)) == 0:
				os.remove(cpath)
			else:
				raise NotEmptyContainerDeletedError(name)

	def exists_container(self, name):
		"""
		Check if the container exists.
		:param name: a container name.
		:return: True or False
		"""

		nname = _normalize_container_name(name)
		cpath = os.path.join(self._path, nname)
		return os.path.isdir(cpath)

	def list_containers(self):
		"""
		Iterator for listing containers within this storage.
		"""

		for path in os.listdir(self._path):
			if os.path.isdir(os.path.join(self._path, path)):
				yield path

class FilesStorageContainer(StorageContainer):

	def __init__(self, storage, name):
		super(FilesStorageContainer, self).__init__(storage, name)

		self._path = os.path.join(storage.path, name)
		_ensure_path_exists(self._path)

	@property
	def path(self):
		return self._path

	def create_object(self, name, **metadata):
		"""
		Create a new object. If an object with this name already exists then just returns it.
		:param name: an object name.
		:return: the object.
		"""

		return FilesStorageObject(self, name, **metadata)

	def get_object(self, name):
		"""
		Return an object by name.
		:param name: the object's name.
		:return: the object.
		"""
		return FilesStorageObject(self, name)

	def delete_object(self, name):
		"""
		Delete an object.
		:param name: an object name.
		"""

		FilesStorageObject(self, name).delete()

	def exists_object(self, name):
		"""
		Check if the object exists.
		:param name: an object name.
		:return: True or False
		"""

		return FilesStorageObject(self, name).exists()

	def list_objects(self, prefix=None, delimiter=None):
		"""
		Iterator for listing objects within this container.
		:param prefix: list only the objects which name starts with this prefix. Useful for pseudo-hierarchical navigation.
		:param delimiter: list distinct object names up to either the first delimiter or the end. Useful for pseudo-hierarchical navigation.
		"""

		listed = set()
		for path, folders, files in os.walk(self._path):
			for filename in files:
				if filename.endswith("__metadata__"):
					continue

				name = os.path.relpath(os.path.join(path, filename), self._path)
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

class FilesStorageObject(StorageObject):

	_BUF_SIZE = 4 * 1024 * 1024 # 4M

	def __init__(self, container, name, **metadata):
		super(FilesStorageObject, self).__init__(container, name, **metadata)

		name = os.path.normpath(name.replace("..", "__"))

		self._path = os.path.join(container.path, name)

		if os.path.exists(self._path) and not os.path.isfile(self._path):
			raise StorageError("Expecting a file at {}".format(cpath))

	@property
	def path(self):
		return self._path

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
		if os.path.exists(self._path):
			if not overwrite:
				raise StorageObjectAlreadyExists(self._name)

			if check_timestamp:
				try:
					with open("{}.__metadata__".format(self._path), "r") as f:
						metadata = json.load(f)
					dst_timestamp = metadata.get("__timestamp__")
					dst_filename = metadata.get("__filename__")
					update = src_filename is None or dst_filename is None or src_timestamp is None or dst_timestamp is None\
									or src_filename != dst_filename or src_timestamp != dst_timestamp
				except:
					pass

		if update:
			_ensure_path_exists(os.path.dirname(self._path))

			with open("{}.__metadata__".format(self._path), "w") as f:
				json.dump(self._metadata, f)

			with open(self._path, "w") as dest:
				buf = src.read(self._BUF_SIZE)
				while len(buf) > 0:
					dest.write(buf)
					buf = src.read(self._BUF_SIZE)

		if close_src:
			src.close()

	def get_data(self, dst=None, check_timestamp=True):
		"""
		Get the object data into the dest file.
		:param dst: Where to save the data. It can be an string with the destination path, a file-like object or None.
		             If it is None a temporary file will be created if the file does not exists already in the local file system.
		:return: The destination path when dest is not a file-like object.
		"""

		if dst is None or isinstance(dst, basestring) and dst == self._path:
			return self._path

		if os.path.exists(self._path):
			with open("{}.__metadata__".format(self._path), "r") as f:
				metadata = json.load(f)
			src_timestamp = metadata.get("__timestamp__")
			src = open(self._path, "r")
		else:
			src_timestamp = None
			src = StringIO("")

		update = True
		if isinstance(dst, basestring):
			dst_filename = dst
			dst_timestamp = os.path.getmtime(dst) if os.path.exists(dst) else None
			update = not check_timestamp or src_timestamp is None or dst_timestamp is None\
						or src_timestamp != dst_timestamp
			if update:
				dst = open(dst, "w")
			close_dest = update
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

		try:
			with open("{}.__metadata__".format(self._path), "r") as f:
				return json.load(f)
		except:
			return {}

	def exists(self):
		"""
		Check whether the object exists.
		:return True or False
		"""

		return os.path.isfile(self._path)

	def delete(self):
		"""
		Delete the object from its container.
		"""

		try:
			os.remove(self._path)
		except:
			pass
		try:
			os.remove("{}.__metadata__".format(self._path))
		except:
			pass
		try:
			dirname = os.path.dirname(self._path)
			while len(os.listdir(dirname)) == 0:
				os.rmdir(dirname)
				dirname = os.path.dirname(dirname)
		except Exception as ex:
			pass