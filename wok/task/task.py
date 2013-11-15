###############################################################################
#
#    Copyright 2009-2013, Universitat Pompeu Fabra
#
#    This file is part of Wok.
#
#    Wok is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Wok is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses
#
###############################################################################

import sys
import types
from datetime import datetime
import signal

from wok import logger
from wok.config.data import Data
from wok.config.optionsconfig import OptionsConfig
from wok.data import data_provider_factory, Stream
from wok.storage import storage_factory
from wok.data.portref import PortDataRef, PORT_MODE_IN, PORT_MODE_OUT

from wok.core.task_result import TaskResult

class Abort(Exception):
	pass

class MissingRequiredPorts(Exception):
	def __init__(self, missing_ports, mode):
		Exception.__init__(self, "Missing required {0} ports: {1}".format(mode, ", ".join(missing_ports)))

# FIXME use wok.errors.MissingConfigParamsError
class MissingRequiredConf(Exception):
	def __init__(self, missing_keys):
		Exception.__init__(self, "Missing required configuration: {0}".format(", ".join(missing_keys)))

class Task(object):
	"""
	Processes a task data partition.
	"""
	
	def __init__(self):

		# Get task key and storage configuration
		cmd_conf = OptionsConfig(required=["case", "task", "index", "data.type", "storage.type"])

		# Register signals
		self._signal_names = {}
		for signame in [x for x in dir(signal) if x.startswith("SIG")]:
			try:
				signum = getattr(signal, signame)
				signal.signal(signum, self.__signal_handler)
				self._signal_names[signum] = signame
			except:
				pass

		# command line configuration
		case_name = cmd_conf["case"]
		task_cname = cmd_conf["task"]
		workitem_index = cmd_conf["index"]

		# initialize the data provider
		provider_conf = cmd_conf["data"]
		self._provider = data_provider_factory.create(provider_conf)
		self._provider.start()

		# initialize storage
		storage_conf = cmd_conf["storage"]
		self.storage = storage_factory.create(storage_conf)
		self.storage = self.storage.get_container(case_name)

		# load the module and task descriptors
		task_desc = self._provider.load_task(case_name, task_cname)
		workitem_desc = self._provider.load_workitem(case_name, task_cname, workitem_index)
		partition = workitem_desc["partition"]

		# setup task configuration
		self.conf = Data.create(task_desc["conf"])
		self.conf["__task_index"] = workitem_index
		self.conf.expand_vars()

		# setup task attributes
		self.case = workitem_desc["case"]
		self.task = workitem_desc["task"]
		self.id = workitem_desc["cname"]
		self.name = workitem_desc["name"]
		self.index = workitem_index

		# initialize decorators
		self._main = None
		self._sources = []
		self._foreach = None
		self._begin = None
		self._end = None

		self._start_time = 0
		self._end_time = self._start_time

		# intialize task logging
		log_conf = self.conf.get("logging")
		logger.initialize(log_conf)
		self.logger = logger.get_logger(self.name)

		self.logger.debug("Task descriptor: {}".format(Data.create(task_desc)))
		self.logger.debug("WorkItem descriptor: {}".format(Data.create(workitem_desc)))

		# Initialize input stream
		self._stream = Stream(self._provider, task_desc["stream"])

		# Initialize ports
		self._ports = {}
		self._in_ports = []
		self._out_ports = []

		self._open_ports = {}

		if "ports" in task_desc and "ports" in partition:
			port_descriptors = Data.create(task_desc["ports"])

			for port_desc in port_descriptors.get("in", default=list):
				port_desc["mode"] = PORT_MODE_IN
				self._ports[port_desc["name"]] = port_desc
				self._in_ports += [port_desc]

			for port_desc in port_descriptors.get("out", default=list):
				port_desc["mode"] = PORT_MODE_OUT
				self._ports[port_desc["name"]] = port_desc
				self._out_ports += [port_desc]

			port_descriptors = Data.create(partition["ports"])

			for port_desc in port_descriptors.get("in", default=list):
				task_port_desc = self._ports[port_desc["name"]]
				task_port_desc["data"] = port_desc["data"]

			for port_desc in port_descriptors.get("out", default=list):
				task_port_desc = self._ports[port_desc["name"]]
				task_port_desc["data"] = port_desc["data"]

		# Get hostname
		try:
			import socket
			self.hostname = socket.gethostname()
		except:
			self.hostname = "unknown"

		# The context field is free to be used by the task user to
		# save variables related with the whole task life cycle.
		# By default it is initialized with a dictionary but can be
		# overwrote with any value by the user. Wok will never use it.
		self.context = {}

	def __signal_handler(self, signum, frame):
		if signum in [signal.SIGABRT, signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
			raise Abort(self._signal_names[signum])
		else:
			self.logger.debug("Received signal {0}".format(self._signal_names[signum]))

	def __dot_product(self, ports):
		names = [port["name"] for port in ports]
		readers = [self.ports(port["name"], mode=PORT_MODE_IN) for port in ports]
		sizes = [readers[i].size() for i in xrange(len(readers))]

		while sum(sizes) > 0:
			data = {}
			for i, reader in enumerate(readers):
				data[names[i]] = reader.read()
				sizes[i] = reader.size()
			yield data

	def __cross_product(self, ports):
		raise Exception("Cross product unimplemented")

	def __default_main(self):

		## Execute before main

		if self._begin:
			self.logger.debug("Running [begin] ...")
			self._begin()

		## Execute sources

		if self._sources:
			self.logger.debug("Running [sources] ...")

			for source in self._sources:
				func, out_ports = source
	
				self.logger.debug("".join([func.__name__,
					"(out_ports=[", ", ".join([p["name"] for p in out_ports]), "])"]))
	
				out_ports = self.ports(*[p["name"] for p in out_ports], mode=PORT_MODE_OUT, iterable=True)
	
				func(*out_ports)

		## Execute foreach's

		if self._foreach is not None:
			self.logger.debug("Running [foreach] ...")

			# initialize foreach
			func, in_ports, out_ports = self._foreach

			writers = []
			writers_index = {}
			for i, port_desc in enumerate(out_ports):
				writer = self.ports(port_desc["name"], mode=PORT_MODE_OUT)
				writers.append(writer)
				writers_index[port_desc["name"]] = i

			self.logger.debug("".join([func.__name__,
				"(in_ports=[", ", ".join([p["name"] for p in in_ports]),
				"], out_ports=[", ", ".join([p["name"] for p in out_ports]), "])"]))

			# determine input port data iteration strategy
			"""
			if self._iter_strategy == "dot":
				iteration_strategy = self.__dot_product
			elif self._iter_strategy == "cross":
				iteration_strategy = self.__cross_product
			else:
				raise Exception("Unknown port data iteration strategy: %s" % self._iter_strategy)
			"""

			iteration_strategy = self.__dot_product
			
			# process each port data iteration element
			for data in iteration_strategy(in_ports):

				params = [data[port_desc["name"]] for port_desc in in_ports]

				ret = func(*params)

				#elif len(out_ports) > 0:
				#	raise Exception("The processor should return the data to write through the output ports: [%s]" % ", ".join([p.name for p in out_ports]))

			for writer in writers:
				writer.close()

		## Execute after main
		if self._end:
			self.logger.debug("Running [end] ...")
			self._end()

		return 0

	def __open_port_data(self, port_desc):
		if port_desc["name"] in self._open_ports:
			return self._open_ports[port_desc["name"]]

		data_ref = PortDataRef.create(port_desc["data"])
		port_data = self._provider.open_port_data(self.case, data_ref)
		self._open_ports[port_desc["name"]] = port_data
		return port_data

	def __ports(self, names=None, mode=None):
		"""
		Return port descriptors by their name.
		"""

		mode = mode or set([PORT_MODE_IN, PORT_MODE_OUT])
		if not isinstance(mode, (list, set)):
			mode = set([mode])
		if not isinstance(mode, set):
			mode = set(mode)

		if names is None or len(names) == 0:
			port_descriptors = []
			if PORT_MODE_IN in mode:
				port_descriptors += self._in_ports
			if PORT_MODE_OUT in mode:
				port_descriptors += self._out_ports
		else:
			port_descriptors = []
			for name in names:
				if name not in self._ports:
					raise Exception("Unknown port: {}".format(name))

				port_desc = self._ports[name]

				if port_desc["mode"] not in mode:
					raise Exception("Incompatible port mode {} for port {}".format(mode, name))

				port_descriptors += [port_desc]

		return port_descriptors

	def elapsed_time(self):
		"Return the elapsed time since the beginning of the task"
		return datetime.now() - self._start_time

	def ports(self, *args, **kargs):
		"""
		Return ports by their name.	Example:
		
			a, b, c = task.ports("a", "b", "c")

		:param mode: Restring the search to ports of this mode.
		:param iterable: Force that the results be iterable either when only one port is requested. Default *False*.
		:return The port data if only one port is requested or a tuple of port data if more than one are requested.
		"""

		mode = kargs.get("mode", None)
		iterable = kargs.get("iterable", False)

		port_descriptors = self.__ports(args, mode=mode)

		ports = [self.__open_port_data(port_desc) for port_desc in port_descriptors]

		if len(ports) == 1 and not iterable:
			return ports[0]
		else:
			return tuple(ports)

	def check_conf(self, keys, exit_on_error=True):
		"""
		Check configuration parameters and return missing keys.

		@param keys A list of keys to be checked
		@exit_on_error Whether to exit if a key is not found
		"""
		missing_keys = []
		for key in keys:
			if key not in self.conf:
				missing_keys += [key]

		if exit_on_error and len(missing_keys) > 0:
			raise MissingRequiredConf(missing_keys)

		return missing_keys

	def run(self):
		"Start the task execution. Remember to call this function at the end of the script !"

		self.logger.debug("Python {0}".format(sys.version))
		self.logger.debug("Task {0} started on host {1}".format(self.id, self.hostname))

		self._start_time = datetime.now()

		aborted = False
		exitcode = -1
		exception = trace = None
		try:
			if self._main is not None:
				self.logger.debug("Running [main] ...")
				exitcode = self._main()
				if exitcode is None:
					exitcode = 0
			else:
				exitcode = self.__default_main()

			self.logger.info("Elapsed time: {0}".format(self.elapsed_time()))
		except Abort as ex:
			self.logger.error("Task aborted with signal {0}".format(ex.args[0]))
			aborted = True
			import traceback
			trace = traceback.format_exc()
		except Exception as ex:
			self.logger.error("Exception on task {0}".format(self.id))
			self.logger.exception(ex)
			exception = str(ex)
			import traceback
			trace = traceback.format_exc()

		result = TaskResult(
			hostname=self.hostname,
			start_time=self._start_time,
			end_time=datetime.now(),
			exitcode=exitcode,
			aborted=aborted,
			exception=exception,
			trace=trace)

		self._provider.save_workitem_result(self.case, self.task, self.index, result)
		self._provider.close()

		exit(exitcode)
		
	def set_main(self, f):
		"Set the main processing function."
		self._main = f

	def main(self, *args):
		"""
		A decorator that is used for specifying which is the task main function. Example::

			@task.main
			def main():
				log = task.logger()
				log.info("Hello world")
		"""
		def decorator(f):
			self.set_main(f)
			return f

		if len(args) == 1 and type(args[0]) in [types.FunctionType, types.MethodType]:
			self.set_main(args[0])
			return args[0]
		else:
			return decorator

	def add_source(self, source_func, out_ports=None):
		"""Add a port data source function"""
		if out_ports is None:
			ports = self.__ports(mode=PORT_MODE_OUT)
		else:
			ports = self.__ports(out_ports, mode=PORT_MODE_OUT)
		if not isinstance(ports, (tuple, list)):
			ports = (ports,)
		self._sources += [(source_func, ports)]

	def source(self, *args, **kwargs):
		"""
		A decorator that is used to define a function that will
		generate port output data. Example::

			@task.source(out_ports=["x", "sum"])
			def sum_n(x, sum):
				N = task.conf["N"]
				s = 0
				for i in xrange(N):
					x.send(i)
					sum.send(s)
					s += i

		:param out_ports: output ports
		"""
		def decorator(f):
			self.add_source(f, **kwargs)
			return f

		if len(args) == 1 and type(args[0]) in [types.FunctionType, types.MethodType]:
			self.add_source(args[0])
			return args[0]
		else:
			return decorator

	def set_foreach(self, processor_func, in_ports=None, out_ports=None):
		"""Set the port data processing function"""
		if in_ports is None:
			iports = self.__ports(mode=PORT_MODE_IN)
		else:
			iports = self.__ports(in_ports, mode=PORT_MODE_IN)
		if not isinstance(iports, (tuple, list)):
			iports = (iports,)
		if out_ports is None:
			oports = self.__ports(mode=PORT_MODE_OUT)
		else:
			oports = self.__ports(out_ports, mode=PORT_MODE_OUT)
		if not isinstance(oports, (tuple, list)):
			oports = (oports,)
		self._foreach = (processor_func, iports, oports)

	def foreach(self, *args, **kwargs):
		"""
		A decorator that is used to specify which is the function that will
		process each port input data. Example::

			@task.foreach(in_ports = ["in1", "in2"])
			def process(name, value):
				return name + " = " + str(value)

		:param in_ports: input ports
		:param out_ports: output ports
		"""
		def decorator(f):
			self.set_foreach(f, **kwargs)
			return f

		if len(args) == 1 and type(args[0]) in [types.FunctionType, types.MethodType]:
			self.set_foreach(args[0])
			return args[0]
		else:
			return decorator

	def set_begin(self, f):
		"""Set the function that will be executed before starting the main function"""
		self._begin = f

	def begin(self, *args):
		"""A decorator that is used to specify the function that will be
		executed before starting the main function"""
		def decorator(f):
			self.set_begin(f)
			return f

		if len(args) == 1 and type(args[0]) in [types.FunctionType, types.MethodType]:
			self.set_begin(args[0])
			return args[0]
		else:
			return decorator

	def set_end(self, f):
		"""Set the function that will be executed after starting the main function"""
		self._end = f

	def end(self, *args):
		"""A decorator that is used to specify the function that will be
		executed after executing the main function"""
		def decorator(f):
			self.set_end(f)
			return f

		if len(args) == 1 and type(args[0]) in [types.FunctionType, types.MethodType]:
			self.set_begin(args[0])
			return args[0]
		else:
			return decorator

# ======================================================================================================================
# Main task ============================================================================================================
# ======================================================================================================================

task = Task()
