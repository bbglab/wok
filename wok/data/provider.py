from wok import logger
from wok.core.errors import UnimplementedError

from stream import Stream

class DataProvider(object):

	def __init__(self, name, conf):
		self._name = name
		self._conf = conf

		self._log = logger.get_logger("wok.data.{}".format(name))

	# ------------------------------------------------------------------------------------------------------------------

	def _port_element(self, port, skip_data=True):
		e = dict(
			name=port.name,
			groupby="",
			streams=["__default__"],
			exclude=False)
		if not skip_data:
			e["data"] = port.data.to_native()
		return e

	def _task_element(self, task):
		# ports

		in_ports = list()
		for i, port_node in enumerate(task.in_ports):
			in_ports.append(self._port_element(port_node))

		out_ports = list()
		for i, port_node in enumerate(task.out_ports):
			out_ports.append(self._port_element(port_node))

		return dict(
			id=task.cname,
			case=task.case.name,
			conf=task.conf.to_native(),
			stream=dict(
					name="__default__",
					join=Stream.JOIN_DOT_PRODUCT),
			ports={"in" : in_ports, "out" : out_ports})

	def _workitem_element(self, workitem):

		"""
		in_ports = list()
		for i, (port_name, port_data) in enumerate(workitem.in_port_data):
			in_ports.append(dict(
				name=port_name,
				data=port_data.to_native()))

		out_ports = list()
		for i, (port_name, port_data) in enumerate(workitem.out_port_data):
			out_ports.append(dict(
				name=port_name,
				data=port_data.to_native()))
		"""

		task = workitem.parent
		return dict(
			case=task.case.name, task=task.cname,
			namespace=workitem.namespace, name=workitem.name, cname=workitem.cname,
			index=workitem.index,
			partition=workitem.partition.to_native())

	# API --------------------------------------------------------------------------------------------------------------

	@property
	def bootstrap_conf(self):
		return dict()

	def start(self):
		raise UnimplementedError()

	def close(self):
		raise UnimplementedError()

	def save_task(self, task):
		raise UnimplementedError()

	def load_task(self, case_name, task_cname):
		raise UnimplementedError()

	def save_workitem(self, case_name, task):
		raise UnimplementedError()

	def load_workitem(self, case_name, task_cname, index):
		raise UnimplementedError()

	def save_workitem_result(self, case_name, task_cname, index, result):
		raise UnimplementedError()

	def load_workitem_result(self, case_name, task_cname, index):
		raise UnimplementedError()

	def open_port_data(self, case_name, data_ref):
		raise UnimplementedError()

	def remove_port_data(self, port):
		raise UnimplementedError()
