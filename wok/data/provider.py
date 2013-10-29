from wok.core.plugin import Plugin

from stream import Stream

class DataProvider(Plugin):

	def __init__(self, conf):
		super(DataProvider, self).__init__(conf, logger_name="wok.data.{}".format(self.plugin_type))

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

	def start(self):
		raise NotImplementedError()

	def close(self):
		raise NotImplementedError()

	def save_task(self, task):
		raise NotImplementedError()

	def load_task(self, case_name, task_cname):
		raise NotImplementedError()

	def save_workitem(self, case_name, task):
		raise NotImplementedError()

	def load_workitem(self, case_name, task_cname, index):
		raise NotImplementedError()

	def save_workitem_result(self, case_name, task_cname, index, result):
		raise NotImplementedError()

	def load_workitem_result(self, case_name, task_cname, index):
		raise NotImplementedError()

	def open_port_data(self, case_name, data_ref):
		raise NotImplementedError()

	def remove_port_data(self, port):
		raise NotImplementedError()
