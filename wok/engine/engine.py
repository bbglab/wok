###############################################################################
#
#    Copyright 2009-2011, Universitat Pompeu Fabra
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

import os
import StringIO
import logging
from Queue import Queue, Empty
import threading
from multiprocessing import cpu_count

from wok import logger
from wok.config import COMMAND_CONF
from wok.config.data import Data, DataList
from wok.config.cli import ConfigBuilder
from wok.core import runstates
from wok.core import events
from wok.core.utils.sync import Synchronizable, synchronized
from wok.core.utils.logs import parse_log
from wok.core.flow.loader import FlowLoader
from wok.core.projects import ProjectManager
from wok.core.storage import StorageContext
from wok.core.storage.factory import create_storage
from wok.platform.factory import create_platform
from wok.jobs import JobSubmission
from wok.jobs import create_job_manager

from instance import Instance, SynchronizedInstance

class WokEngine(Synchronizable):
	"""
	The Wok engine manages the execution of workflow instances.
	Each instance represents a workflow loaded with a certain configuration.
	"""

	def __init__(self, conf):
		Synchronizable.__init__(self)

		self._global_conf = conf

		self._conf = wok_conf = conf["wok"]

		self._log = logger.get_logger("wok.engine", conf=wok_conf.get("log"))

		self._work_path = wok_conf.get("work_path", os.path.join(os.getcwd(), "wok"))
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		self._flow_path = wok_conf.get("flow_path")
		if self._flow_path is None:
			self._flow_path = [os.curdir]
		elif isinstance(self._flow_path, basestring):
			self._flow_path = [self._flow_path]
		elif not isinstance(self._flow_path, (list, DataList)):
			raise Exception('wok.flow_path: A list of paths expected. Example ["path1", "path2"]')

		"""
		self._flow_loader = FlowLoader(self._flow_path)
		sb = ["wok.flow_path:\n"]
		for uri, ff in self._flow_loader.flow_files.items():
			sb += ["\t", uri, "\t", ff[0], "\n"]
		self._log.debug("".join(sb))
		"""

		self._instances = []
		self._instances_map = {}

		self._stopping_instances = {}

		#self._lock = Lock()
		self._cvar = threading.Condition(self._lock)
		
		self._run_thread = None
		self._running = False

		self._finished_event = threading.Event()

		self._job_task_map = {}

		self._logs_threads = []
		self._logs_queue = Queue()

		self._join_thread = None
		self._join_queue = Queue()

		self._notified = False

		#db_path = os.path.join(self._work_path, "engine.db")
		#self._db = create_db("sqlite:///{}".format(db_path))

		self._platform = self._create_platform()
		#self._job_manager = self._create_job_manager()

		self._storage = self._create_storage(wok_conf)

		self._projects = ProjectManager(self._conf.get("projects"))
		self._projects.initialize()

		self._restore_state()

	def _create_platform(self):
		"""
		Creates the platform according to the configuration
		:return: Platform
		"""

		conf = self._conf.get("platform")
		if conf is None:
			conf = Data.element()
		elif not Data.is_element(conf):
			self._log.error("Wrong configuration type for 'platform': {}".format(conf))
			conf = Data.element()

		name = conf.get("type", "local")

		if "work_path" not in conf:
			conf["work_path"] = os.path.join(self._work_path, "platform", name)

		self._log.info("Creating '{}' platform ...".format(name))
		self._log.debug("Platform configuration: {}".format(repr(conf)))

		return create_platform(name, conf)

	'''
	def _create_job_manager(self):
		"""
		Creates the job manager according to the configuration
		:return: JobManager
		"""

		name = "mcore"

		conf = Data.element()
		if "jobs" in self._conf:
			conf.merge(self._conf["jobs"])
			if "type" in conf:
				name = conf["type"]
				del conf["type"]

		if "work_path" not in conf:
			conf["work_path"] = os.path.join(self._work_path, "jobs", name)

		self._log.info("Creating '{}' job manager ...".format(name))
		self._log.debug("Job manager configuration: {}".format(repr(conf)))

		return create_job_manager(name, conf)
	'''

	@staticmethod
	def _create_storage(wok_conf):
		storage_conf = wok_conf.get("storage")
		if storage_conf is None:
			storage_conf = wok_conf.element()

		storage_type = storage_conf.get("type", "sfs")

		if "work_path" not in storage_conf:
			wok_work_path = wok_conf.get("work_path", os.path.join(os.getcwd(), "wok"))
			storage_conf["work_path"] = os.path.join(wok_work_path, "storage")

		return create_storage(storage_type, StorageContext.CONTROLLER, storage_conf)

	def _on_job_update(self, event, **kwargs):
		self.notify()

	def _restore_state(self):
		#TODO restore db state
		pass

	def _instance_job_ids(self, instance):
		return [job_id for job_id, task in self._job_task_map.items() if task.instance == instance]

	def __queue_adaptative_get(self, queue, start_timeout = 1, max_timeout = 4):
		timeout = start_timeout
		msg = None
		while self._running and msg is None:
			try:
				msg = queue.get(timeout=timeout)
			except Empty:
				if timeout < max_timeout:
					timeout += 1
		return msg

	def __queue_batch_get(self, queue, start_timeout = 1, max_timeout = 4):
		timeout = start_timeout
		msg_batch = []
		while self._running and len(msg_batch) == 0:
			try:
				msg_batch += [queue.get(timeout=timeout)]
				while not queue.empty():
					msg_batch += [queue.get(timeout=timeout)]
			except Empty:
				if timeout < max_timeout:
					timeout += 1
		return msg_batch

	# threads ----------------------

	@synchronized
	def _run(self):
		self._running = True

		num_exc = 0

		# Start the logs threads

		for i in range(cpu_count()):
		#for i in range(1):
			t = threading.Thread(target=self._logs, args=(i, ), name="wok-engine-logs-%d" % i)
			self._logs_threads += [t]
			t.start()

		# Start the join thread

		self._join_thread = threading.Thread(target=self._join, name="wok-engine-join")
		self._join_thread.start()

		log_conf = self._conf.get("log")
		if log_conf is not None:
			log_conf = log_conf.clone().expand_vars(context=self._global_conf)
		_log = logger.get_logger("wok.engine.run", conf=log_conf)

		_log.debug("Engine run thread ready")

		while self._running:
			try:
				#_log.debug("Scheduling new tasks ...")

				# submit tasks ready to be executed
				for inst in self._instances:
					tasks = inst.schedule()
					#submissions = self.__job_submissions(tasks)
					for js, job_id in self._platform.submit(tasks):
						self._job_task_map[job_id] = js.task
				
				#_log.debug("Waiting for events ...")

				while not self._notified and self._running:
					self._cvar.wait(1)
				self._notified = False

				if not self._running:
					break

				#_log.debug("Stoping jobs for aborting instances ...")

				# check stopping instances
				for inst in self._instances:
					if inst.state == runstates.ABORTING and \
								inst not in self._stopping_instances:
							job_ids = inst.job_ids()
							if len(job_ids) == 0:
								inst.state = runstates.ABORTED
							else:
								_log.info("Stopping {} jobs for instance {} ...".format(len(job_ids), inst.name))
								self._stopping_instances[inst] = set(job_ids)
								self._platform.jobs.abort(job_ids)

				#_log.debug("Checking job state changes ...")

				updated_modules = set()

				# detect tasks which state has changed
				for job_id, state in self._platform.jobs.state():
					task = self._job_task_map[job_id]
					if task.state != state:
						task.state = state
						updated_modules.add(task.parent)
						_log.debug("Task {} changed state to {}".format(task.id, state))

						# if task has finished, queue it for logs retrieval
						# otherwise queue it directly for joining
						if state in [runstates.FINISHED, runstates.FAILED, runstates.ABORTED]:
							self._logs_queue.put((task, job_id))

				#_log.debug("Updating modules state ...\n" + "\n".join("\t{}".format(m.id) for m in sorted(updated_modules)))
				#_log.debug("Updating modules state ...")

				# update affected modules state
				updated_instances = set()
				for m in updated_modules:
					inst = m.instance
					inst.update_module_state_from_children(m)
					_log.debug("Module {} updated state to {} ...".format(m.id, m.state))
					updated_instances.add(inst)

					if self._log.isEnabledFor(logging.INFO):
						count = m.update_tasks_count_by_state()
						sb = ["[{}] {} ({})".format(m.instance.name, m.id, m.state.title)]
						sep = " "
						for state in runstates.STATES:
							if state.title in count:
								sb += [sep, "{}={}".format(state.symbol, count[state.title])]
								if sep == " ":
									sep = ", "
						self._log.info("".join(sb))

				for inst in updated_instances:
				#	self._log.debug(repr(inst))
					inst.update_state()

			except Exception:
				num_exc += 1
				_log.exception("Exception in wok-engine run thread (%d)" % num_exc)

		try:
			# print instances state before leaving the thread
			#for inst in self._instances:
			#	_log.debug("Instances state:\n" + repr(inst))

			for t in self._logs_threads:
				t.join()

			self._join_thread.join()

			_log.debug("Engine run thread finished")
		except:
			pass
		
		self._running = False

	def _logs(self, index):
		"Log retrieval thread"

		log_conf = self._conf.get("log")
		if log_conf is not None:
			log_conf = log_conf.clone().expand_vars(context = self._global_conf)
		_log = logger.get_logger("wok.engine.logs-%d" % index, conf = log_conf)
		
		_log.debug("Engine logs thread ready")

		num_exc = 0

		while self._running:
			# get the next task to retrieve the logs
			job_info = self.__queue_adaptative_get(self._logs_queue)
			if job_info is None:
				continue

			task, job_id = job_info

			try:
				_log.debug("Reading logs for task %s ..." % task.id)

				task.state_msg = "Reading logs"
				output = self._platform.jobs.output(job_id)
				if output is None:
					output = StringIO.StringIO()

				task_index = task.index
				module = task.parent
				module_id = module.id
				instance_name = module.instance.name
				logs_storage = self._storage.logs

				logs = []
#				last_timestamp = None
				line = output.readline()
				while len(line) > 0:
					timestamp, level, name, text = parse_log(line)
#					if timestamp is None:
#						timestamp = last_timestamp
#					last_timestamp = timestamp

					logs += [(timestamp, level, name, text)]

					line = output.readline()

					if len(line) == 0 or len(logs) >= 1000:
						logs_storage.append(instance_name, module_id, task_index, logs)
						
						#self._log.debug("Task %s partial logs:\n%s" % (task.id, "\n".join("\t".join(log) for log in logs)))

						logs = []
			except:
				num_exc += 1
				_log.exception("Exception in wok-engine logs thread (%d)" % num_exc)

			finally:
				_log.debug("Done with logs of task %s" % task.id)
				self._join_queue.put(job_info)

		#self._db.clean()

		_log.debug("Engine logs thread finished")

	def _join(self):
		"Joiner thread"

		log_conf = self._conf.get("log")
		if log_conf is not None:
			log_conf = log_conf.clone().expand_vars(context=self._global_conf)
		_log = logger.get_logger("wok.engine.join", conf=log_conf)

		_log.debug("Engine join thread ready")

		num_exc = 0

		while self._running:
			try:
				job_info = self.__queue_adaptative_get(self._join_queue)
				if job_info is None:
					continue

				task, job_id = job_info

				#_log.debug("Joining task %s ..." % task.id)
				with self._lock:
					task.job_result = self._platform.jobs.join(job_id)
					del self._job_task_map[job_id]
					task.state_msg = ""

					if self._single_run and len(self._job_task_map) == 0:
						stop_engine = True
						for inst in self._instances:
							stop_engine = stop_engine and (inst.state in [runstates.FINISHED, runstates.FAILED])
						#self._running = not stop_engine
						if stop_engine:
							self._finished_event.set()

					_log.debug("Task %s joined" % task.id)

					# check stopping instances
					inst = task.instance
					if inst in self._stopping_instances:
						job_ids = self._stopping_instances[inst]
						if job_id in job_ids:
							job_ids.remove(job_id)
						if len(job_ids) == 0:
							del self._stopping_instances[inst]
							inst.state = runstates.ABORTED

			except:
				num_exc += 1
				_log.exception("Exception in wok-engine join thread (%d)" % num_exc)

		_log.debug("Engine join thread finished")

	# API -----------------------------------

	@property
	def conf(self):
		return self._conf

	@property
	def work_path(self):
		return self._work_path

	@property
	def storage(self):
		return self._storage

	@property
	def projects(self):
		return self._projects

	"""
	@property
	def flow_loader(self):
		return self._flow_loader
	"""

	@synchronized
	def start(self, wait=True, single_run=False):
		self._log.info("Starting engine ...")

		self._platform.start()
		self._platform.callbacks.add(events.JOB_UPDATE, self._on_job_update)

		#self._job_manager.start()
		#self._job_manager.add_callback(self._on_job_update)

		for project in self._projects:
			self._platform.sync_project(project)

		self._single_run = single_run
		
		self._run_thread = threading.Thread(target = self._run, name = "wok-engine-run")
		self._run_thread.start()

		self._log.info("Engine started")

		if wait:
			self._lock.release()
			self.wait()
			self._lock.acquire()

	def wait(self):
		self._log.info("Waiting for the engine to finish ...")

		try:
			finished = self._finished_event.wait(1)
			while not finished:
				finished = self._finished_event.wait(1)
		except KeyboardInterrupt:
			self._log.warn("Ctrl-C pressed ...")
		except Exception as e:
			self._log.error("Exception while waiting for the engine to finish, stopping the engine ...")
			self._log.exception(e)

		self._log.info("Finished waiting for the engine ...")

	def _stop_threads(self):
		self._log.info("Stopping threads ...")

		if self._run_thread is not None:

			with self._lock:
				self._running = False
				self._cvar.notify()

			while self._run_thread.isAlive():
				try:
					self._run_thread.join(1)
				except KeyboardInterrupt:
					self._log.warn("Ctrl-C pressed, killing the process ...")
					import signal
					os.kill(os.getpid(), signal.SIGTERM)
				except Exception as e:
					self._log.error("Exception while waiting for threads to finish ...")
					self._log.exception(e)
					self._log.warn("killing the process ...")
					import signal
					os.kill(os.getpid(), signal.SIGTERM)

			self._run_thread = None

		self._log.info("All threads finished ...")

	@synchronized
	def stop(self):
		self._log.info("Stopping the engine ...")

		self._finished_event.set()

		self._lock.release()

		#self._job_manager.close()

		self._platform.close()

		if self._run_thread is not None:
			self._stop_threads()

		self._lock.acquire()

		self._log.info("Engine stopped")

	def notify(self, lock=True):
		if lock:
			self._lock.acquire()
		self._notified = True
		self._cvar.notify()
		if lock:
			self._lock.release()

	@synchronized
	def instances(self):
		instances = []
		for inst in self._instances:
			instances += [SynchronizedInstance(self, inst)]
		return instances
	
	@synchronized
	def instance(self, name):
		inst = self._instances_map.get(name)
		if inst is None:
			return None
		return SynchronizedInstance(self, inst)

	@synchronized
	def create_instance(self, inst_name, conf_builder, flow_uri):
		"Creates a new workflow instance"

		#TODO check in the db
		if inst_name in self._instances_map:
			raise Exception("Instance with this name already exists: {}".format(inst_name))

		self._log.debug("Creating instance {} from {} ...".format(inst_name, flow_uri))

		# Create instance
		inst = Instance(inst_name, conf_builder, flow_uri, engine=self, platform=self._platform)
		try:
			# Initialize instance and register by name
			inst.initialize()
			self._instances += [inst]
			self._instances_map[inst_name] = inst

			self._storage.clean(inst)

			self._cvar.notify()

			#TODO self._db.instance_persist(inst)
		except:
			self._log.error("Error while creating instance {} for the workflow {} with configuration {}".format(inst_name, flow_uri, cb()))
			raise

		self._log.debug("\n" + repr(inst))

		return SynchronizedInstance(self, inst)

	@synchronized
	def remove_instance(self, name):
		if name in self._instances_map:
			inst = self._instances_map[name]
			del self._instances_map[name]
			self._instances.remove(inst)

			self._platform.jobs.abort(job_ids=self._instance_job_ids(inst))

			self._storage.clean(inst)
