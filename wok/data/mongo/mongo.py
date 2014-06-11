import pymongo

from wok.data.provider import DataProvider
from wok.data.portref import PORT_MODE_IN, PORT_MODE_OUT
from wok.core.task_result import TaskResult

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

	plugin_type = "mongo"

	def __init__(self, conf):
		super(MongoProvider, self).__init__(conf)

		# http://docs.mongodb.org/manual/reference/connection-string/
		self._url = conf.get("url", "mongodb://localhost")
		self._database = conf.get("database", "wok")

		self._client = None
		self._db = None

	def __case_collection(self, case_name):
		return self._db[case_name]
	
	def __tasks_collection(self, case_name):
		return self.__case_collection(case_name).tasks

	def __workitem_desc_collection(self, case_name):
		return self.__case_collection(case_name).workitem_descriptors

	def __workitem_result_collection(self, case_name):
		return self.__case_collection(case_name).workitem_result

	def __port_data_collection(self, case_name, task_cname=None, port_name=None):
		c = self.__case_collection(case_name).port_data
		if task_cname is None:
			return c
		c = c[task_cname]
		if port_name is None:
			return c
		return c[port_name]

	def start(self):
		self._log.debug("Connecting to {} ...".format(self._url))

		self._client = pymongo.MongoClient(self._url)
		if isinstance(self._client, pymongo.database.Database):
			self._db = self._client
			if database is not None:
				raise StorageError("Database already specified in the url")
		else:
			self._db = self._client[self._database]

	def close(self):
		if self._client is not None:
			self._log.debug("Closing connection to {} ...".format(self._url))
			self._client.close()
			self._client = None

	def create_case(self, case_name):
		pass

	def remove_case(self, case_name):
		self.__case_collection(case_name).drop()
		self.__tasks_collection(case_name).drop()
		self.__workitem_desc_collection(case_name).drop()
		self.__workitem_result_collection(case_name).drop()
		self.__port_data_collection(case_name).drop()

	def save_task(self, task):
		task_coll = self.__tasks_collection(task.case.name)
		e = self._task_element(task)
		e["_id"] = task.cname
		task_coll.save(e)

	def load_task(self, case_name, task_cname):
		task_coll = self.__tasks_collection(case_name)
		task = task_coll.find_one({"_id" : task_cname})
		del task["_id"]
		return task

	def save_workitem(self, workitem):
		task = workitem.parent
		workitem_coll = self.__workitem_desc_collection(task.case.name)
		e = self._workitem_element(workitem)
		e["_id"] = "{}-{:08}".format(task.cname, workitem.index)
		workitem_coll.save(e)

	def load_workitem(self, case_name, task_cname, index):
		workitem_coll = self.__workitem_desc_collection(case_name)
		workitem = workitem_coll.find_one({"_id" : "{}-{:08}".format(task_cname, index)})
		del workitem["_id"]
		return workitem

	def save_workitem_result(self, case_name, task_cname, index, result):
		workitem_result_coll = self.__workitem_result_collection(case_name)
		e = result.to_native()
		e["_id"] = "{}-{:08}".format(task_cname, index)
		workitem_result_coll.save(e)

	def load_workitem_result(self, case_name, task_cname, index):
		workitem_result_coll = self.__workitem_result_collection(case_name)
		result = workitem_result_coll.find_one({"_id" : "{}-{:08}".format(task_cname, index)})
		if result is not None:
			return TaskResult.from_native(result)
		else:
			return TaskResult()

	def open_port_data(self, case_name, data_ref):
		if data_ref.mode == PORT_MODE_IN:
			port_coll = []
			for ref in data_ref.refs:
				port_coll += [self.__port_data_collection(case_name, ref.component_cname, ref.port_name)]
			return MongoInPort(self, port_coll, data_ref.start, data_ref.size)
		elif data_ref.mode == PORT_MODE_OUT:
			port_coll = self.__port_data_collection(case_name, data_ref.component_cname, data_ref.port_name)
			return MongoOutPort(self, port_coll, data_ref.partition_index)

	def remove_port_data(self, port):
		task = port.parent
		port_coll = self.__port_data_collection(task.case.name, task.cname, port.name)
		port_coll.drop()