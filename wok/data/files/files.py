import os
import shutil
import json

from wok.data.provider import DataProvider
from wok.data.portref import PORT_MODE_IN, PORT_MODE_OUT
from wok.core.task_result import TaskResult

from port import SourceData, FilesInPort, FilesOutPort

class FilesProvider(DataProvider):
	"""
	SQLite data provider.

	Configuration namespace: **wok.platform.data**
	    **work_path**: Path were database files will be created.
	    **log**: logging configuration
	"""

	DUMP_OPTIONS = dict(
		indent=True
	)

	plugin_type = "files"

	plugin_required_conf = ["path"]

	def __init__(self, conf):
		super(FilesProvider, self).__init__(conf)

		self._path = conf["path"]

	def __case_path(self, case_name, create=True):
		case_path = os.path.join(self._path, case_name)
		if create and not os.path.exists(case_path):
			os.makedirs(case_path)
		return case_path

	def __task_path(self, case_name, task_cname, create=True):
		case_path = self.__case_path(case_name, create=create)
		task_path = os.path.join(case_path, task_cname)
		if create and not os.path.exists(task_path):
			os.makedirs(task_path)
		return task_path

	def __workitems_path(self, case_name, task_cname, create=True):
		task_path = self.__task_path(case_name, task_cname)
		workitems_path = os.path.join(task_path, "workitems")
		if create and not os.path.exists(workitems_path):
			os.makedirs(workitems_path)
		return workitems_path

	def __port_path(self, case_name, component_cname, port_name, create=True):
		task_path = self.__task_path(case_name, component_cname)
		ports_path = os.path.join(task_path, "ports")
		if create and not os.path.exists(ports_path):
			os.makedirs(ports_path)
		return os.path.join(ports_path, "{}.db".format(port_name))

	def start(self):
		if not os.path.exists(self._path):
			self._log.debug("Creating path {} ...".format(self._path))
			os.makedirs(self._path)

	def close(self):
		pass

	def create_case(self, case_name):
		case_path = self.__case_path(case_name, create=True)

	def remove_case(self, case_name):
		case_path = self.__case_path(case_name, create=False)
		if os.path.exists(case_path):
			self._log.debug("Removing data for case {} from {} ...".format(case_name, case_path))
			shutil.rmtree(case_path)

	def save_task(self, task):
		task_path = self.__task_path(task.case.name, task.cname)
		with open(os.path.join(task_path, "task.json"), "w") as f:
			json.dump(self._task_element(task), f, self.DUMP_OPTIONS)

	def load_task(self, case_name, task_cname):
		task_path = self.__task_path(case_name, task_cname)
		with open(os.path.join(task_path, "task.json"), "r") as f:
			return json.load(f)

	def save_workitem(self, workitem):
		task = workitem.parent
		workitems_path = self.__workitems_path(task.case.name, task.cname)
		file_name = "{:08}.json".format(workitem.index)
		with open(os.path.join(workitems_path, file_name), "w") as f:
			json.dump(self._workitem_element(workitem), f, self.DUMP_OPTIONS)

	def load_workitem(self, case_name, task_cname, index):
		workitems_path = self.__workitems_path(case_name, task_cname)
		file_name = "{:08}.json".format(index)
		with open(os.path.join(workitems_path, file_name), "r") as f:
			return json.load(f)

	def save_workitem_result(self, case_name, task_cname, index, result):
		workitems_path = self.__workitems_path(case_name, task_cname)
		file_name = "{:08}-result.json".format(index)
		with open(os.path.join(workitems_path, file_name), "w") as f:
			json.dump(result.to_native(), f, self.DUMP_OPTIONS)

	def load_workitem_result(self, case_name, task_cname, index):
		workitems_path = self.__workitems_path(case_name, task_cname)
		file_name = "{:08}-result.json".format(index)
		try:
			with open(os.path.join(workitems_path, file_name), "r") as f:
				return TaskResult.from_native(json.load(f))
		except:
			return TaskResult()

	def open_port_data(self, case_name, data_ref):
		if data_ref.mode == PORT_MODE_IN:
			port_sources = []
			for ref in data_ref.refs:
				port_source = SourceData(self.__port_path(case_name, ref.component_cname, ref.port_name))
				port_sources += [port_source]
			return FilesInPort(self, port_sources, data_ref.start, data_ref.size)
		elif data_ref.mode == PORT_MODE_OUT:
			port_source = SourceData(self.__port_path(case_name, data_ref.component_cname, data_ref.port_name))
			return FilesOutPort(self, port_source, data_ref.partition_index)

	def remove_port_data(self, port):
		task = port.parent
		port_path = self.__port_path(task.case.name, task.cname, port.name)
		if os.path.exists(port_path):
			shutil.rmtree(port_path)