import os
from fnmatch import fnmatch

from wok.core.plugin import Plugin
from wok.logger import get_logger

class StorageError(Exception):
	pass

class ObjectAlreadyExistsError(Exception):
	pass

class NotEmptyContainerDeletedError(Exception):
	def __init__(self, name):
		Exception.__init__(self, "Can not delete a container that have objects inside: {}".format(name))


class Storage(Plugin):
	def __init__(self, conf):
		super(Storage, self).__init__(conf)

		self.name = conf.get("name", self.plugin_type)

	def create_container(self, name):
		"""
		Creates a new container. If a container with this name already exists then it just returns it.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""
		raise NotImplementedError()

	def get_container(self, name):
		"""
		Returns a container. If no container exists then a new one will be created.
		:param name: the name of the container. It can not contain "/".
		:return: the container
		"""
		raise NotImplementedError()

	def delete_container(self, name, delete_objects=True):
		"""
		Deletes a container. Nothing happens if it doesn't exist. Fails if it contains objects unless delete_objects is True.
		:param name: the name of the container. It can not contain "/".
		:param delete_objects: If the container has objects then delete them before.
		"""
		raise NotImplementedError()

	def exists_container(self, name):
		"""
		Check if the container exists.
		:param name: a container name.
		:return: True or False
		"""
		raise NotImplementedError()

	def list_containers(self):
		"""
		Iterator for listing containers within this storage.
		"""
		raise NotImplementedError()

class StorageContainer(object):
	def __init__(self, storage, name):
		self._storage = storage
		self._name = name

	@property
	def storage(self):
		return self._storage

	@property
	def name(self):
		return self._name

	def create_object(self, name, **metadata):
		"""
		Create a new object. If an object with this name already exists then just returns it.
		:param name: an object name.
		:return: the object.
		"""
		raise NotImplementedError()

	def get_object(self, name):
		"""
		Return an object by name. If the object doesn't exist it returns an empty object.
		:param name: the object's name.
		:return: the object.
		"""
		raise NotImplementedError()

	def delete_object(self, name):
		"""
		Delete an object.
		:param name: an object name.
		"""
		raise NotImplementedError()

	def exists_object(self, name):
		"""
		Check if the object exists.
		:param name: an object name.
		:return: True or False
		"""
		raise NotImplementedError()

	def list_objects(self, prefix=None, delimiter=None):
		"""
		Iterator for listing objects within this container.
		:param prefix: list only the objects which name starts with this prefix. Useful for pseudo-hierarchical navigation.
		:param delimiter: list distinct object names up to either the first delimiter or the end. Useful for pseudo-hierarchical navigation.
		"""
		raise NotImplementedError()

	def upload(self, source_path, object_prefix="", include=None, exclude=None, start_callback=None, **kwargs):
		"""
		Uploads all the files found in a given local path into the container as objects.
		:param source_path: source path
		:param object_prefix: name prefix for all the objects
		:param include: the list of files to include. If it is None include all files not explicitly excluded. It can contain globs.
		:param exclude: the list of files to exclude.
		:param start_callback: a callable to notify the upload start. As argument receives the relative file path.
		:param kwargs: optional arguments passed to put_data()
		"""

		def walk(source_path):
			if os.path.isfile(source_path):
				base_path = os.path.dirname(source_path)
				yield base_path, base_path, [os.path.basename(source_path)]
			else:
				for path, folders, files in os.walk(source_path):
					yield source_path, path, files

		for base_path, path, files in walk(source_path):
			rel_path = os.path.relpath(path, base_path) if path != base_path else ""
			for filename in files:
				rel_file_path = os.path.join(rel_path, filename)
				if include is not None and not self.__filename_match(rel_file_path, include)\
						or exclude is not None and self.__filename_match(rel_file_path, exclude):
					continue

				if start_callback is not None:
					start_callback(rel_file_path)

				obj = self.create_object(os.path.join(object_prefix, rel_file_path))
				obj.put_data(os.path.join(path, filename), **kwargs)

	def download(self, target_path, prefix=None, delimiter=None, include=None, exclude=None, start_callback=None):
		"""
		Download to a local path the selected objects in the container.
		:param prefix: download only the objects which name starts with this prefix. Useful for pseudo-hierarchical navigation.
		:param delimiter: download distinct objects with names up to either the first delimiter or the end. Useful for pseudo-hierarchical navigation.
		"""

		for obj_name in self.list_objects(prefix, delimiter):
			if include is not None and not self.__filename_match(obj_name, include)\
					or exclude is not None and self.__filename_match(obj_name, exclude):
				continue

			path = os.path.normpath(obj_name)
			path = os.path.normpath("{}/{}".format(target_path, obj_name))
			if os.path.relpath(path, target_path).startswith(".."):
				get_logger(__name__).warn("Object output path goes outside the target path: {}".format(path))
				continue

			#print "***", obj_name, "--->", path

			base_path = os.path.dirname(path)
			if not os.path.exists(base_path):
				os.makedirs(base_path)

			prefixed_obj_name = (prefix or "") + obj_name
			obj = self.get_object(prefixed_obj_name)
			if obj.exists():
				start_callback(obj_name)
				obj.get_data(path)

	@staticmethod
	def __filename_match(filename, patterns):
		for pattern in patterns:
			if fnmatch(filename, pattern):
				return True
		return False

	def repr(self):
		return "{}://{}".format(
			self._storage.name,
			self._name)

class StorageObject(object):
	def __init__(self, container, name, **metadata):
		self._container = container
		self._name = name
		self._metadata = dict(metadata)

	@property
	def container(self):
		return self._container

	@property
	def name(self):
		return self._name

	def put_data(self, src, overwrite=True):
		"""
		Put data into the object from a source.

		:param src: It can be the path to a file to get data from or a file-like object.
		:param overwrite: whether to overwrite the object if it already exists or fail.
		"""
		raise NotImplementedError()

	def get_data(self, dest=None):
		"""
		Get the object data into the dest file.
		:param dest: Where to save the data. It can be an string with the destination path, a file-like object or None.
		             If it is None a temporary file will be created if the file does not exists already in the local file system.
		:return: The destination path when dest is not a file-like object.
		"""
		raise NotImplementedError()

	def get_metadata(self):
		"""
		Get the object metadata.
		:return a dictionary with the metadata
		"""
		raise NotImplementedError()

	def exists(self):
		"""
		Check whether the object exists.
		:return True or False
		"""
		raise NotImplementedError()

	def delete(self):
		"""
		Delete the object from its container.
		"""
		raise NotImplementedError()

	def repr(self):
		return "{}/{}".format(repr(self._container), self._name)
