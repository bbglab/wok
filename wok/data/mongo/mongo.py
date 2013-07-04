import pymongo

from wok.data.provider import DataProvider
from wok.data.portref import PORT_MODE_IN, PORT_MODE_OUT

from port import MongoInPort, MongoOutPort

class MongoProvider(DataProvider):
	"""
	MongoDB data provider.

	Configuration namespace: **wok.platform.data**
	    **url**: connection url.
	    **exec_url**: connection url from execution nodes. By default the same as *url*.
	    **database**: The database name.
	    **log**: logging configuration
	"""

	def __init__(self, conf):
		DataProvider.__init__(self, "mongo", conf)

		self._url = conf.get("url", "mongodb://localhost")
		self._exec_url = conf.get("exec_url", self._url)
		self._database = conf.get("database", "wok")

		self._client = None
		self._db = None

	def __modules_collection(self, instance_name):
		return self._db[instance_name].modules

	def __task_desc_collection(self, instance_name):
		return self._db[instance_name].task_descriptors

	def __task_results_collection(self, instance_name):
		return self._db[instance_name].task_results

	def __port_data_collection(self, instance_name, module_path, port_name):
		return self._db[instance_name].port_data[module_path][port_name]

	@property
	def bootstrap_conf(self):
		return dict(
			type=self._conf["type"],
			url=self._exec_url,
			database=self._database)

	def start(self):
		self._log.debug("Connecting to {} ...".format(self._url))

		self._client = pymongo.MongoClient(self._url)
		self._db = self._client[self._database]

	def close(self):
		if self._client is not None:
			self._log.debug("Closing connection to {} ...".format(self._url))
			self._client.close()
			self._client = None

	def save_module(self, module):
		mod_coll = self.__modules_collection(module.instance.name)
		e = self._module_element(module)
		e["_id"] = module.id
		mod_coll.save(e)

	def load_module(self, instance_name, module_id):
		mod_coll = self.__modules_collection(instance_name)
		module = mod_coll.find_one({"_id" : module_id})
		del module["_id"]
		return module

	def save_task(self, task):
		task_coll = self.__task_desc_collection(task.instance.name)
		e = self._task_element(task)
		e["_id"] = "{}-{:08}".format(task.parent.id, task.index)
		task_coll.save(e)

	def load_task(self, instance_name, module_id, task_index):
		task_coll = self.__task_desc_collection(instance_name)
		task = task_coll.find_one({"_id" : "{}-{:08}".format(module_id, task_index)})
		del task["_id"]
		return task

	def open_port_data(self, instance_name, data_ref):
		port_coll = self.__port_data_collection(instance_name, data_ref.module_path, data_ref.port_name)
		if data_ref.mode == PORT_MODE_IN:
			return MongoInPort(self, data_ref.port_name, port_coll, data_ref.start, data_ref.size)
		elif data_ref.mode == PORT_MODE_OUT:
			return MongoOutPort(self, data_ref.port_name, port_coll, data_ref._partition)

	def remove_port_data(self, port):
		module = port.parent
		port_coll = self.__port_data_collection(module.instance.name, module.id, port.name)
		port_coll.drop()