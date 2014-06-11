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

saga = __import__("saga")

import os
import time
import threading

from datetime import datetime

import sqlalchemy.types as types
from sqlalchemy import Column, Integer, String, Float

from wok.config import JOBS_CONF
from wok.config.data import Data
from wok.core import runstates
from wok.core.errors import ConfigTypeError
from wok.core.utils.proctitle import set_thread_title

from wok.jobs import Job, JobManager

_SAGA_STATES = [
	saga.job.UNKNOWN,
	saga.job.NEW,
	saga.job.PENDING,
	saga.job.RUNNING,
	saga.job.SUSPENDED,
	saga.job.DONE,
	saga.job.FAILED,
	saga.job.CANCELED
]

_SAGA_STATES_INDEX = {}
for i, state in enumerate(_SAGA_STATES):
	_SAGA_STATES_INDEX[state] = i

class SagaState(types.TypeDecorator):

	impl = types.Integer

	def process_bind_param(self, value, dialect):
		return _SAGA_STATES_INDEX[value] if value is not None else None

	def process_result_value(self, value, dialect):
		return _SAGA_STATES[value] if value is not None else None

class SagaJob(Job):
	saga_id = Column(String)
	saga_state = Column(SagaState)
	saga_getjob_start = Column(Float)

	def _repr(self, sb):
		if self.saga_id is not None:
			sb += [",saga_id={}".format(self.saga_id)]
		if self.saga_state is not None:
			sb += [",saga_state={}".format(self.saga_state)]
		if self.saga_getjob_start is not None:
			sb += [",saga_getjob_start={}".format(self.saga_getjob_start)]

def datetime_from_posix(value):
	if value is None:
		return None
	try:
		return datetime.strptime(value, "%a %b %d %H:%M:%S %Y")
	except:
		return None

class SagaJobManager(JobManager):
	"""
	SAGA implementation of a job manager.

	Configuration::
	    **remote_path**: The remote working path.
	    **url**: SAGA Job service url.
	    **context**: The security context information. If many contexts are required then this can be a list of contexts.
	    **pe**: Default parallel environment for submitted jobs. Default none.
	    **cpu_count**: Default number of cpu's to reserve for the jobs submitted to the parallel environment. Default 1.
	    **queue**: Default queue for submitted jobs. Default none.
	    **project**: Default project for submitted jobs. Default none.
	    **working_directory**: Default job working directory. Default none.
	    **state_check_interval**: Interval in seconds between checks for jobs state. Default 5.
	"""

	job_class = SagaJob

	__JOB_STATE_FROM_SAGA = {
		saga.job.NEW 		: runstates.WAITING,
		saga.job.PENDING 	: runstates.WAITING,
		saga.job.RUNNING	: runstates.RUNNING,
		saga.job.SUSPENDED	: runstates.WAITING,
		saga.job.DONE		: runstates.FINISHED,
		saga.job.FAILED		: runstates.FAILED,
		saga.job.CANCELED	: runstates.ABORTED,
        saga.job.UNKNOWN    : runstates.UNKNOWN
	}

	def __init__(self, conf):
		JobManager.__init__(self, "saga", conf)

		self._file_url = self._conf.get("files_url", "file://")
		self._remote_path = self._conf.get("remote_path", self._work_path)

		self._output_path = os.path.join(self._work_path, "output")
		self._remote_output_path = os.path.join(self._remote_path, "output")

		self._pe = self._conf.get("pe")
		self._cpu_count = self._conf.get("cpu_count", 1)

		self._queue = self._conf.get("queue")
		self._project = self._conf.get("project")

		self._working_directory = self._conf.get("working_directory")

		self._state_check_interval = self._conf.get("state_check_interval", 5)

		ctx_conf = self._conf.get("context")
		if ctx_conf is not None and not (Data.is_element(ctx_conf) or Data.is_list(ctx_conf)):
			raise ConfigTypeError("context", ctx_conf)

		self._session = None
		self._job_service = None

		self._queued_count = 0
		self._max_queued = self._conf.get("max_queued", 0)

		self._running = False
		self._run_thread = None
		self._join_thread = None

	def _task_conf(self, job, key, default=None):
		return JobManager._task_conf(self, job, key, JOBS_CONF, default)

	def _start(self):

		self._log.debug("Creating session ...")

		self._session = saga.Session()

		ctxs_conf = self._conf.get("context")
		if ctxs_conf is not None:
			if Data.is_element(ctxs_conf):
				ctxs_conf = Data.list([ctxs_conf])

			for ctx_conf in ctxs_conf:
				try:
					ctx = saga.Context(ctx_conf["type"])
					for key in ctx_conf:
						if hasattr(ctx, key):
							setattr(ctx, key, ctx_conf[key])
					self._session.add_context(ctx)
				except Exception as ex:
					self._log.error("Wrong context configuration: {}".format(repr(ctx_conf)))
					self._log.exception(ex)

		self._log.debug("Creating job service ...")

		url = self._conf.get("service_url", "fork://localhost", dtype=str)
		self._job_service = saga.job.Service(url, session=self._session)

		self._remote_dir = saga.filesystem.Directory(self._file_url, session=self._session)

		# FIXME Use the logging configuration mechanisms of SAGA
		from wok import logger
		logger.init_logger("SGEJobService", conf=Data.element(dict(level=self._conf.get("saga_log.level", "error"))))

		# TODO count the number of previously queued jobs

		# TODO clean output files ?

		self._running = True
		self._run_thread = threading.Thread(target=self._run_handler, name="{}-run".format(self._name))
		self._join_thread = threading.Thread(target=self._join_handler, name="{}-join".format(self._name))
		self._run_thread.start()
		self._join_thread.start()

	#TODO def _attach(self, session, job):

	def _run_handler(self):

		set_thread_title()

		session = self._create_session()

		with self._lock:
			while self._running:

				if self._max_queued != 0 and self._queued_count >= self._max_queued:
					self._lock.release()
					time.sleep(1)
					self._lock.acquire()
					continue

				job = None

				try:
					#self._log.debug("Looking for waiting jobs ...")
					job = session.query(SagaJob).filter(
							SagaJob.state == runstates.WAITING,
							SagaJob.saga_state == None).order_by(SagaJob.priority).first()

					if job is not None:
						pass #self._queued_count += 1
						#self._log.debug("Found: {}".format(repr(job)))
				except Exception as e:
					self._log.exception(e)

				if job is None:
					self._lock.release()
					time.sleep(1)
					self._lock.acquire()
					continue

				self._lock.release()

				jd = saga.job.Description()
				jd.name = job.name

				jd.executable = job.script
				jd.arguments = []
				jd.environment = job.env

				jd.spmd_variation = self._task_conf(job, "pe", self._pe)
				jd.total_cpu_count = self._task_conf(job, "cpu_count", self._cpu_count)

				jd.queue = self._task_conf(job, "queue", self._queue)
				jd.project = self._task_conf(job, "project", self._project)
				jd.working_directory = self._task_conf(job, "working_directory", self._working_directory)

				jd.output = jd.error = os.path.join(self._remote_output_path, job.case_name, "{}.txt".format(job.name))

				# remove previous output file
				try:
					self._log.debug("Removing remote output for [{}] {} ...".format(job.id, job.name))
					self._remote_dir.remove(jd.output)
				except saga.DoesNotExist:
					pass
				except Exception as ex:
					self._log.error("Couldn't remove remote job output file: {}".format(ex))

				self._lock.acquire()

				# create and run the job
				try:
					self._log.debug("Running job [{}] {} ...".format(job.id, job.name))
					saga_job = None
					saga_job = self._job_service.create_job(jd)
					saga_job.run()

					job.saga_id = saga_job.id
					job.saga_state = saga_job.state
					job.output = jd.output
					session.commit()

					self._queued_count += 1

				except Exception as ex:
					self._log.exception(ex)
					if saga_job is not None: # and saga_job.state in [saga.job.RUNNING, saga.job.SUSPENDED]:
						try:
							saga_job.cancel()
						finally:
							session.rollback()

		session.close()

		self._log.debug("Run thread finished")

	def _join_handler(self):

		set_thread_title()

		session = self._create_session()

        # TODO make it more robust to exceptions
		with self._lock:
			while self._running:

				#self._log.debug("Checking for the state of jobs ...")

				for job in session.query(SagaJob).filter(
								SagaJob.saga_state != None,
								~SagaJob.state.in_(runstates.TERMINAL_STATES)):

					#self._log.debug("Checking state for [{}] {} ... ".format(job.id, job.name))

					try:
						if job.saga_getjob_start is None:
							job.saga_getjob_start = time.time()
							session.commit()

						#self._qlock.release()
						saga_job = self._job_service.get_job(job.saga_id)
						#self._qlock.acquire()

						job.saga_state = saga_job.state

						next_state = self.__JOB_STATE_FROM_SAGA[job.saga_state]
						if job.state == next_state:
							continue

						if job.saga_state in [saga.job.DONE, saga.job.FAILED, saga.job.CANCELED]:
							job.exitcode = saga_job.exit_code
							job.created = datetime_from_posix(saga_job.created)
							job.started = datetime_from_posix(saga_job.started)
							job.finished = datetime_from_posix(saga_job.finished)
							job.hostname = saga_job.execution_hosts

					except saga.NoSuccess:
						# wait up to N seconds before failing
						if time.time() - job.saga_getjob_start >= 120:
							self._log.error("Couldn't reconnect to job {}".format(job.saga_id))
							# what should we do ? failing or finishing ?
							job.state = runstates.FAILED # let's be paranoid !
							session.commit()

							self._lock.release()
							self._fire_job_updated(job)
							self._lock.acquire()
						continue

					if job.saga_state == saga.job.DONE and job.exitcode != 0:
						next_state = runstates.FAILED

					if job.state != runstates.WAITING and next_state == runstates.WAITING:
						self._queued_count += 1
					elif job.state == runstates.WAITING and next_state != runstates.WAITING:
						self._queued_count -= 1

					job.state = next_state

					session.commit()

					self._lock.release()
					self._fire_job_updated(job)
					self._lock.acquire()

					if not self._running:
						break

				if self._running:
					self._lock.release()
					time.sleep(self._state_check_interval)
					self._lock.acquire()

		session.close()

		self._log.debug("Join thread finished")

	def _output(self, job):

		local_path = os.path.join(self._output_path, job.case_name)
		if not os.path.exists(local_path):
			os.makedirs(local_path)
		local_path = os.path.join(local_path, os.path.basename(job.output))
		local_url = "file://{}".format(local_path)

		#self._log.error("\n[remote] {}\n[local ] {}".format(job.output, local_path))

		try:
			self._lock.release()
			#self._log.debug("Retrieving task output ...\n  From: {}\n  To  : {}".format(remote_url, local_path))
			self._remote_dir.copy(job.output, local_url)
			return open(local_path)
		except Exception as ex:
			self._log.error(str(ex))
			from traceback import format_exc
			self._log.debug(format_exc(ex))
			return None
		finally:
			self._lock.acquire()

	def _join(self, session, job):
		try:
			saga_job = self._job_service.get_job(job.saga_id)
		except saga.NoSuccess:
			self._log.error("Error getting SAGA job during the join: {}".format(job.saga_id))
			return

		timedout = False
		while self._running and not timedout:
			timedout = not saga_job.wait(timeout=1.0)

	def _close(self):

		self._log.debug("Closing threads ...")

		self._running = False

		self._lock.release()

		if self._run_thread is not None:
			while self._run_thread.isAlive():
				self._run_thread.join(1)
		if self._join_thread is not None:
			while self._join_thread.isAlive():
				self._join_thread.join(1)

		self._lock.acquire()

		self._log.debug("Closing session ...")

		if self._job_service is not None:
			self._job_service.close()
			self._job_service = None

		if self._session is not None:
			self._session = None