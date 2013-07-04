from wok import logger
from wok.core.errors import UnimplementedError

from stream import Stream

class DataProvider(object):

	def __init__(self, name, conf):
		self._name = name
		self._conf = conf

		self._log = logger.get_logger(name="wok.data.{}".format(name), conf=conf.get("log"))

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

	def _module_element(self, module):
		# ports

		in_ports = list()
		for i, port_node in enumerate(module.in_ports):
			in_ports.append(self._port_element(port_node))

		out_ports = list()
		for i, port_node in enumerate(module.out_ports):
			out_ports.append(self._port_element(port_node))

		return dict(
			id=module.id,
			instance=module.instance.name,
			conf=module.conf.to_native(),
			stream=dict(
					name="__default__",
					join=Stream.JOIN_DOT_PRODUCT),
			ports={"in" : in_ports, "out" : out_ports})

	def _task_element(self, task):
		# TODO stream partition info
		partition = dict()

		in_ports = list()
		for i, (port_name, port_data) in enumerate(task.in_port_data):
			in_ports.append(dict(
				name=port_name,
				data=port_data.to_native()))

		out_ports = list()
		for i, (port_name, port_data) in enumerate(task.out_port_data):
			out_ports.append(dict(
				name=port_name,
				data=port_data.to_native()))

		return dict(
			id=task.id, name=task.name, index=task.index,
			module=task.parent.id, instance=task.instance.name,
			partition=partition, ports={"in" : in_ports, "out" : out_ports})

	# API --------------------------------------------------------------------------------------------------------------

	@property
	def bootstrap_conf(self):
		return dict()

	def start(self):
		raise UnimplementedError()

	def close(self):
		raise UnimplementedError()

	def save_module(self, module):
		raise UnimplementedError()

	def load_module(self, instance_name, module_id):
		raise UnimplementedError()

	def save_task(self, task):
		raise UnimplementedError()

	def load_task(self, instance_name, module_id, task_index):
		raise UnimplementedError()

	def save_task_result(self, result):
		raise UnimplementedError()

	def load_task_result(self, instance_name, module_id, task_index):
		raise UnimplementedError()

