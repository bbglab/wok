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
import shutil
import StringIO
import time
from Queue import Queue, Empty
import threading
from multiprocessing import cpu_count

from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from blinker import Signal

from wok import logger
from wok.config.data import Data
from wok.config.loader import ConfigLoader
from wok.core import runstates
from wok.core import events
from wok.core import errors
from wok.core.utils.sync import Synchronizable, synchronized
from wok.core.utils.atomic import AtomicCounter
from wok.core.utils.logsdb import LogsDb
from wok.core.utils.proctitle import set_thread_title
from wok.engine.projects import ProjectManager
from wok.platform.factory import create_platform
from wok.core.cmd import create_command_builder
from wok.jobs import JobSubmission

import db
from case import Case, SynchronizedCase

_DT_FORMAT = "%Y-%m-%d %H:%M:%S"

class WokEngine(Synchronizable):
	"""
	The Wok engine manages the execution of workflow cases.
	Each case represents a workflow loaded with a certain configuration.
	"""

	def __init__(self, conf, conf_base_path=None):
		Synchronizable.__init__(self)

		self._global_conf = conf

		self._expanded_global_conf = conf.clone().expand_vars()

		self._conf = self._expanded_global_conf.get("wok", default=Data.element)

		self._conf_base_path = conf_base_path

		self._log = logger.get_logger("wok.engine")

		self._work_path = self._conf.get("work_path", os.path.join(os.getcwd(), "wok-files"))
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		self._cases = []
		self._cases_by_name = {}

		self._stopping_cases = {}

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

		self._num_log_threads = self._conf.get("num_log_threads", cpu_count())
		self._max_alive_threads = 2 + self._num_log_threads
		self._num_alive_threads = AtomicCounter()

		self._started = False

		self._notified = False

		recover = self._conf.get("recover", False)

		db_path = os.path.join(self._work_path, "engine.db")
		if not recover and os.path.exists(db_path):
			os.remove(db_path)
		self._db = db.create_engine("sqlite:///{}".format(db_path), drop_tables=not recover)

		# platforms

		self._platforms = self._create_platforms()
		self._platforms_by_name = {}
		for platform in self._platforms:
			self._platforms_by_name[platform.name] = platform
		default_platform_name = self._conf.get("default_platform", self._platforms[0].name)
		if default_platform_name not in self._platforms_by_name:
			self._log.warn("Platform '{}' not found, using '{}' as the default platform".format(
				default_platform_name, self._platforms[0].name))
			default_platform_name = self._platforms[0].name
		self._default_platform = self._platforms_by_name[default_platform_name]

		# projects

		if conf_base_path is None:
			conf_base_path = os.getcwd()
		projects_conf = self._global_conf.get("wok.projects")
		self._projects = ProjectManager(projects_conf, base_path=conf_base_path)
		self._projects.initialize()

		# signals

		self.case_created = Signal()
		self.case_state_changed = Signal()
		self.case_started = Signal()
		self.case_finished = Signal()
		self.case_removed = Signal()

		# recovering
		if recover:
			self.__recover_from_db()

	def _create_platforms(self):
		"""
		Creates the platform according to the configuration
		:return: Platform
		"""

		platform_confs = self._conf.get("platforms")
		if platform_confs is None:
			platform_confs = Data.list()
		elif not Data.is_list(platform_confs):
			self._log.error("Wrong configuration type for 'platforms': {}".format(platform_confs))
			platform_confs = Data.list()

		if len(platform_confs) == 0:
			platform_confs += [Data.element(dict(type="local"))]

		platforms = []

		names = {}
		for pidx, platform_conf in enumerate(platform_confs):
			if isinstance(platform_conf, basestring):
				if not os.path.isabs(platform_conf) and self._conf_base_path is not None:
					platform_conf = os.path.join(self._conf_base_path, platform_conf)
				platform_conf = ConfigLoader(platform_conf).load()

			if not Data.is_element(platform_conf):
				raise errors.ConfigTypeError("wok.platforms[{}]".format(pidx, platform_conf))

			ptype = platform_conf.get("type", "local")

			name = platform_conf.get("name", ptype)
			if name in names:
				name = "{}-{}".format(name, names[name])
				names[name] += 1
			else:
				names[name] = 2
			platform_conf["name"] = name

			if "work_path" not in platform_conf:
				platform_conf["work_path"] = os.path.join(self._work_path, "platform_{}".format(name))

			self._log.info("Creating '{}' platform ...".format(name))
			self._log.debug("Platform configuration: {}".format(repr(platform_conf)))

			platforms += [create_platform(ptype, platform_conf)]

		return platforms

	def _on_job_update(self, event, **kwargs):
		self.notify()

	def __recover_from_db(self):
		raise NotImplementedError()

	def __queue_adaptative_get(self, queue, start_timeout=1.0, max_timeout=6.0):
		timeout = start_timeout
		msg = None
		while self._running and msg is None:
			try:
				msg = queue.get(timeout=timeout)
			except Empty:
				if timeout < max_timeout:
					timeout += 0.5
			except:
				break
		return msg

	# Not used anywhere
	def __queue_batch_get(self, queue, start_timeout=1, max_timeout=5):
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

	def __job_submissions(self, session, platform):
		#FIXME Be fair with priorities between different cases ?
		query = session.query(db.WorkItem)\
			.filter(db.WorkItem.state == runstates.READY)\
			.filter(db.WorkItem.platform == platform.name)\
			.order_by(db.WorkItem.priority)

		for workitem in query:
			case = self._cases_by_name[workitem.case.name]
			task = case.component(workitem.task.cname)

			js = JobSubmission(
					case=case,
					task=task,
					workitem_id=workitem.id,
					job_name=workitem.cname,
					task_conf=task.conf,
					priority=workitem.priority)

			execution = task.execution
			cmd_builder = create_command_builder(execution.mode)
			js.script, js.env = cmd_builder.prepare(case, task, workitem.index)

			yield js

	def __remove_case(self, session, case):
		"""
		Definitively remove a case. The engine should be locked and no case jobs running.
		"""

		self._log.info("Dropping case {} ...".format(case.name))

		del self._cases_by_name[case.name]
		self._cases.remove(case)

		# remove engine db objects and finalize case
		self._log.debug("  * database ...")
		case.remove(session)

		self._lock.release()
		try:
			#TODO clean the job manager output files

			try:
				self._log.debug("  * logs ...")
				logs_path = os.path.join(self._work_path, "logs", case.name)
				shutil.rmtree(logs_path)
			except:
				self._log.exception("Error removing logs at {}".format(logs_path))

			# remove data
			self._log.debug("  * data ...")
			for platform in case.platforms:
				platform.data.remove_case(case.name)

			# remove storage
			self._log.debug("  * storage ...")
			for platform in case.platforms:
				platform.storage.delete_container(case.name)

			# emit signal
			self.case_removed.send(case)

		finally:
			self._lock.acquire()

	# threads ----------------------

	@synchronized
	def _run(self):

		set_thread_title()

		num_exc = 0

		self._running = True

		self._num_alive_threads += 1

		# Start the logs threads

		for i in range(self._num_log_threads):
			t = threading.Thread(target=self._logs, args=(i, ), name="wok-engine-logs-%d" % i)
			self._logs_threads += [t]
			t.start()

		# Start the join thread

		self._join_thread = threading.Thread(target=self._join, name="wok-engine-join")
		self._join_thread.start()

		_log = logger.get_logger("wok.engine.run")

		_log.debug("Engine run thread ready")

		while self._running:

			session = db.Session()

			try:
				#_log.debug("Scheduling new tasks ...")
				set_thread_title("scheduling")

				updated_tasks = set()

				# schedule tasks ready to be executed and save new workitems into the db
				for case in self._cases:
					tasks = case.schedule(session)
					updated_tasks.update(tasks)
					session.commit()

				# submit workitems ready to be executed
				for platform in self._platforms:
					job_submissions = self.__job_submissions(session, platform)
					for js, job_id, job_state in platform.submit(job_submissions):
						workitem = session.query(db.WorkItem).filter(db.WorkItem.id == js.workitem_id).one()
						workitem.job_id = job_id
						workitem.state = job_state
						js.task.dirty = True
						session.commit()
						updated_tasks.add(js.task)

				session.close()
				session = None

				#_log.debug("Waiting for events ...")

				set_thread_title("waiting")

				while len(updated_tasks) == 0 and not self._notified and self._running:
					self._cvar.wait(1)
				self._notified = False

				if not self._running:
					break

				session = db.Session() # there is a session.close() in the finished block

				#_log.debug("Stopping jobs for aborting instances ...")

				set_thread_title("working")

				# check stopping instances
				for case in self._cases:
					if (case.state == runstates.ABORTING or case.removed) and case not in self._stopping_cases:
						num_job_ids = session.query(db.WorkItem.job_id).filter(db.WorkItem.case_id == case.id)\
											.filter(~db.WorkItem.state.in_(runstates.TERMINAL_STATES)).count()
						if num_job_ids == 0:
							if case.state == runstates.ABORTING:
								_log.debug("Aborted case {} with no running jobs".format(case.name))
								dbcase = session.query(db.Case).filter(db.Case.id == case.id)
								dbcase.state = case.state = runstates.ABORTED
								session.commit()
							else:
								_log.debug("Stopped case {} with no running jobs".format(case.name))

							if case.removed:
								_log.debug("Removing case {} with no running jobs".format(case.name))
								self.__remove_case(session, case)
								session.commit()
						else:
							_log.info("Stopping {} jobs for case {} ...".format(num_job_ids, case.name))

							self._stopping_cases[case] = set()
							for platform in self._platforms:
								job_ids = [int(r[0]) for r in session.query(db.WorkItem.job_id)
															.filter(db.WorkItem.case_id == case.id)\
															.filter(db.WorkItem.platform == platform.name)\
															.filter(~db.WorkItem.state.in_(runstates.TERMINAL_STATES))]

								self._stopping_cases[case].update(job_ids)

								platform.jobs.abort(job_ids)

				#_log.debug("Checking job state changes ...")

				# detect workitems which state has changed
				for platform in self._platforms:
					for job_id, state in platform.jobs.state():
						try:
							workitem = session.query(db.WorkItem).filter(db.WorkItem.job_id == job_id).one()
						except NoResultFound:
							_log.warn("No work-item available for the job {0} while retrieving state".format(job_id))
							platform.jobs.abort([job_id])
							platform.jobs.join(job_id)
							continue

						if workitem.state != state:
							case = self._cases_by_name[workitem.case.name]
							task = case.component(workitem.task.cname)
							task.dirty = True

							workitem.state = state
							workitem.substate = runstates.LOGS_RETRIEVAL
							session.commit()
							updated_tasks.add(task)

							# if workitem has finished, queue it for logs retrieval
							if state in runstates.TERMINAL_STATES:
								self._logs_queue.put((workitem.id, job_id))

							_log.debug("[{}] Work-Item {} changed state to {}".format(case.name, workitem.cname, state))

				#_log.debug("Updating components state ...")

				# update affected components state
				updated_cases = set([task.case for task in updated_tasks])
				for case in updated_cases:
					case.update_states(session)
					case.update_count_by_state(session)
					case.clean_components(session)
					session.commit()

					if case.state == runstates.RUNNING:
						self._lock.release()
						try:
							self.case_started.send(case)
						finally:
							self._lock.acquire()

				for task in updated_tasks:
					case = task.case
					#_log.debug("[{}] Component {} updated state to {} ...".format(
					#				component.case.name, component.cname, component.state))

					count = task.workitem_count_by_state
					sb = ["[{}] {} ({})".format(case.name, task.cname, task.state.title)]
					sep = " "
					for state in runstates.STATES:
						if state in count:
							sb += [sep, "{}={}".format(state.symbol, count[state])]
							if sep == " ":
								sep = ", "

					if task.state == runstates.FINISHED and task.state in count:
						elapsed = str(task.elapsed)
						elapsed = elapsed.split(".")[0]
						sb += [" ", "<{}>".format(elapsed)]

					self._log.info("".join(sb))

			except BaseException as ex:
				num_exc += 1
				_log.warn("Exception in run thread ({}): {}".format(num_exc, str(ex)))
				#if num_exc > 3:
				#	raise
				#else:
				from traceback import format_exc
				_log.debug(format_exc())

				try:
					if session is not None:
						session.rollback()
				except Exception as ex:
					_log.warn("Session rollback failed")
					_log.exception(ex)

			finally:
				try:
					if session is not None:
						session.close()
				except Exception as ex:
					_log.warn("Session close failed")
					_log.exception(ex)

				session = None

		set_thread_title("finishing")

		try:
			# print cases state before leaving the thread
			#for case in self._cases:
			#	_log.debug("Case state:\n" + repr(case))

			for t in self._logs_threads:
				t.join()

			self._lock.release()
			self._join_thread.join()
			self._lock.acquire()

			_log.debug("Engine run thread finished")
		except Exception as ex:
			_log.exception(ex)
		
		self._running = False
		self._num_alive_threads -= 1

	def _logs(self, index):
		"Log retrieval thread"

		set_thread_title()

		self._num_alive_threads += 1

		_log = logger.get_logger("wok.engine.logs-{}".format(index))
		
		_log.debug("Engine logs thread ready")

		num_exc = 0

		while self._running:
			set_thread_title("waiting")

			# get the next task to retrieve the logs
			job_info = self.__queue_adaptative_get(self._logs_queue)
			if job_info is None:
				continue

			workitem_id, job_id = job_info

			session = db.Session()

			task = None
			try:
				workitem = session.query(db.WorkItem).filter(db.WorkItem.id == workitem_id).one()

				case = self._cases_by_name[workitem.case.name]
				task = case.component(workitem.task.cname)

				set_thread_title(workitem.cname)

				_log.debug("[{}] Reading logs for work-item {} ...".format(case.name, workitem.cname))

				output = task.platform.jobs.output(job_id)
				if output is None:
					output = StringIO.StringIO()

				path = os.path.join(self._work_path, "logs", case.name, task.cname)
				if not os.path.isdir(path):
					try:
						os.makedirs(path)
					except:
						if not os.path.isdir(path):
							raise

				path = os.path.join(path, "{:08}.db".format(workitem.index))
				if os.path.isfile(path):
					os.remove(path)

				logs_db = LogsDb(path)
				logs_db.open()
				logs_db.add(case.name, task.cname, workitem.index, output)
				logs_db.close()

				_log.debug("[{}] Done with logs of work-item {}".format(case.name, workitem.cname))

			except BaseException as ex:
				num_exc += 1
				session.rollback()
				_log.info("Exception in logs thread ({}): {}".format(num_exc, str(ex)))
				from traceback import format_exc
				_log.debug(format_exc())

			finally:
				workitem.substate = runstates.JOINING
				self._join_queue.put(job_info)
				session.commit()
				session.close()

		self._num_alive_threads -= 1

		_log.debug("Engine logs thread finished")

	def _join(self):
		"Joiner thread"

		set_thread_title()

		self._num_alive_threads += 1

		_log = logger.get_logger("wok.engine.join")

		_log.debug("Engine join thread ready")

		session = None

		num_exc = 0

		while self._running:
			try:
				set_thread_title("waiting")

				job_info = self.__queue_adaptative_get(self._join_queue)
				if job_info is None:
					continue

				workitem_id, job_id = job_info

				with self._lock:
					session = db.Session()

					workitem = session.query(db.WorkItem).filter(db.WorkItem.id == workitem_id).one()

					case = self._cases_by_name[workitem.case.name]
					task = case.component(workitem.task.cname)

					set_thread_title(task.cname)

					#_log.debug("Joining work-item %s ..." % task.cname)

					jr = task.platform.jobs.join(job_id)

					wr = Data.element(dict(
							hostname=jr.hostname,
							created=jr.created.strftime(_DT_FORMAT) if jr.created is not None else None,
							started=jr.started.strftime(_DT_FORMAT) if jr.started is not None else None,
							finished=jr.finished.strftime(_DT_FORMAT) if jr.finished is not None else None,
							exitcode=jr.exitcode.code if jr.exitcode is not None else None))

					r = task.platform.data.load_workitem_result(case.name, task.cname, workitem.index)

					if r is not None:
						if r.exception is not None:
							wr["exception"] = r.exception
						if r.trace is not None:
							wr["trace"] = r.trace

					workitem.substate = None
					workitem.result = wr

					case.num_active_workitems -= 1

					session.commit()

					# check if there are still more work-items
					num_workitems = session.query(func.count(db.WorkItem.id)).filter(
						~db.WorkItem.state.in_(runstates.TERMINAL_STATES)).scalar()

					if self._single_run and num_workitems == 0:
						stop_engine = True
						for case in self._cases:
							stop_engine = stop_engine and (case.state in runstates.TERMINAL_STATES)
						#self._running = not stop_engine
						if stop_engine:
							self._finished_event.set()

					_log.debug("[{}] Joined work-item {}".format(case.name, workitem.cname))

					# check stopping instances
					if case in self._stopping_cases:
						job_ids = self._stopping_cases[case]
						if job_id in job_ids:
							job_ids.remove(job_id)

						if len(job_ids) == 0:
							del self._stopping_cases[case]
							if case.state == runstates.ABORTING:
								workitem.case.state = case.state = runstates.ABORTED

							session.commit()

							if case.removed:
								self.__remove_case(session, case)
								session.commit()
						else:
							_log.debug("Still waiting for {} jobs to stop".format(len(job_ids)))

					if case.state in runstates.TERMINAL_STATES and case.num_active_workitems == 0:
						_log.info("[{}] Case {}. Total time: {}".format(case.name, case.state.title, str(case.elapsed)))

						self._lock.release()
						try:
							self.case_finished.send(case)
						finally:
							self._lock.acquire()

			except BaseException as ex:
				num_exc += 1
				_log.warn("Exception in join thread ({}): {}".format(num_exc, str(ex)))
				from traceback import format_exc
				_log.debug(format_exc())

				try:
					if session is not None:
						session.rollback()
				except Exception as ex:
					_log.warn("Session rollback failed")
					_log.exception(ex)

			finally:
				try:
					if session is not None:
						session.close()
				except Exception as ex:
					_log.warn("Session close failed")
					_log.exception(ex)

		self._num_alive_threads -= 1

		_log.debug("Engine join thread finished")

	# API -----------------------------------

	@property
	def conf(self):
		return self._conf

	@property
	def work_path(self):
		return self._work_path

	@property
	def projects(self):
		return self._projects

	def platform(self, name):
		return self._platforms_by_name.get(name)

	@property
	def default_platform(self):
		return self._default_platform

	@synchronized
	def start(self, wait=True, single_run=False):
		self._log.info("Starting engine ...")

		started_platforms = []
		try:
			for platform in self._platforms:
				started_platforms += [platform]
				platform.start()
				platform.callbacks.add(events.JOB_UPDATE, self._on_job_update)
		except BaseException as ex:
			self._log.error(str(ex))
			for platform in started_platforms:
				platform.close()
			raise

		#for project in self._projects:
		#	self._default_platform.sync_project(project)

		self._single_run = single_run
		
		self._run_thread = threading.Thread(target=self._run, name="wok-engine-run")
		self._run_thread.start()

		self._lock.release()
		try:
			try:
				self._num_alive_threads.wait_condition(lambda value: value < self._max_alive_threads)

				self._started = True

				self._log.info("Engine started")
			except KeyboardInterrupt:
				wait = False
				self._log.warn("Ctrl-C pressed ...")
			except Exception as e:
				wait = False
				self._log.error("Exception while waiting for the engine to start")
				self._log.exception(e)

			if wait:
				self.wait()
		finally:
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
					exit(-1)
					import signal
					os.kill(os.getpid(), signal.SIGTERM)

			self._run_thread = None

		self._log.info("All threads finished ...")

	@synchronized
	def stop(self):
		self._log.info("Stopping the engine ...")

		self._finished_event.set()

		self._lock.release()
		try:
			if self._run_thread is not None:
				self._stop_threads()

			for platform in self._platforms:
				platform.close()
		finally:
			self._lock.acquire()

		self._started = False

		self._log.info("Engine stopped")

	def running(self):
		return self._started

	def notify(self, lock=True):
		if lock:
			self._lock.acquire()
		self._notified = True
		self._cvar.notify()
		if lock:
			self._lock.release()

	@synchronized
	def cases(self):
		instances = []
		for inst in self._cases:
			instances += [SynchronizedCase(self, inst)]
		return instances
	
	@synchronized
	def case(self, name):
		inst = self._cases_by_name.get(name)
		if inst is None:
			return None
		return SynchronizedCase(self, inst)

	@synchronized
	def exists_case(self, name):
		return name in self._cases_by_name

	@synchronized
	def create_case(self, case_name, conf_builder, project_name, flow_name, container_name):
		"Creates a new workflow case"

		session = db.Session()
		if session.query(db.Case).filter(db.Case.name==case_name).count() > 0:
			raise Exception("A case with this name already exists: {}".format(case_name))

		flow_uri = "{}:{}".format(project_name, flow_name)

		self._log.info("Creating case {} from {} ...".format(case_name, flow_uri))

		try:
			try:
				flow = self.projects.load_flow(flow_uri)
				project = flow.project
			except:
				self._log.error("Error while loading the workflow from {}".format(flow_uri))
				raise

			for platform in self._platforms:
				try:
					platform.data.remove_case(case_name)
					platform.data.create_case(case_name)
				except:
					self._log.error("Error while initializing data for case {}".format(case_name))
					raise

			try:
				case = Case(case_name, conf_builder, project, flow, container_name, engine=self)

				self._cases += [case]
				self._cases_by_name[case_name] = case

				case.persist(session)

				session.flush()
				self.notify(lock=False)
			except:
				self._log.error("Error while creating case {} for the workflow {} with configuration {}".format(
					case_name, flow_uri, conf_builder.get_conf()))
				raise
		except:
			session.rollback()
			#self._log.error("Error while creating case {} for the workflow {} with configuration {}".format(
			#	case_name, flow_uri, conf_builder.get_conf()))
			raise

		session.close()

		self._log.debug("\n" + repr(case))

		self._lock.release()
		try:
			self.case_created.send(case)
		finally:
			self._lock.acquire()

		return SynchronizedCase(self, case)

	@synchronized
	def remove_case(self, name):
		if name in self._cases_by_name:
			session = db.Session()
			case = self._cases_by_name[name]
			dbcase = session.query(db.Case).filter(db.Case.id == case.id).first()
			dbcase.removed = case.removed = True
			if case.state not in runstates.TERMINAL_STATES + [runstates.READY]:
				dbcase.state = case.state = runstates.ABORTING

			num_retries = 3
			while num_retries > 0:
				try:
					session.commit()
					self.notify(lock=False)
					self._log.debug("Case {} marked for removal".format(case.name))
				except BaseException as ex:
					num_retries -= 1
					_log.info("Exception in remove_case: {}".format(str(ex)))
					if num_retries > 0:
						_log.info("Remaining retries = {}".format(num_retries))
						import time
						time.sleep(1)
					else:
						from traceback import format_exc
						_log.debug(format_exc())
						session.rollback()
				finally:
					session.close()

		else:
			self._log.error("Trying to remove a non existing case: {}".format(name))

		'''
			try:
				# retrieve job id's
				session = db.Session()
				job_ids = session.query(db.WorkItem.job_id)\
								.filter(db.WorkItem.case_id == self.id)\
								.filter(db.WorkItem.job_id != None).all()

				# abort running jobs
				case.platform.jobs.abort(job_ids)
			except:
				self._log.error("Remove case {}: Error aborting jobs.".format(name))

			#TODO we need to know when the jobs finish to clean the engine log dbs and the job manager output files

			try:
				# remove data
				case.platform.data.remove_case(case.name)
			except:
				self._log.error("Remove case {}: Error removing data.".format(name))

			# remove engine db objects and finalize case
			#case.remove(session)

			#TODO we need to know when the jobs finish to clean the engine db objects and finalize the case

			session.close()
		'''
