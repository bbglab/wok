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

import sqlalchemy.types as types
from sqlalchemy import Column, Integer, String

from wok.config import JOBS_CONF
from wok.config.data import Data
from wok.core.errors import ConfigTypeError
from wok.core import runstates

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

	def _repr(self, sb):
		if self.saga_id is not None:
			sb += [",saga_id={}".format(self.saga_id)]
		if self.saga_state is not None:
			sb += [",saga_state={}".format(self.saga_state)]

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

	__JOB_STATE_FROM_SAGA = {
		saga.job.NEW 		: runstates.WAITING,
		saga.job.PENDING 	: runstates.WAITING,
		saga.job.RUNNING	: runstates.RUNNING,
		saga.job.SUSPENDED	: runstates.WAITING,
		saga.job.DONE		: runstates.FINISHED,
		saga.job.FAILED		: runstates.FAILED,
		saga.job.CANCELED	: runstates.ABORTED
	}

	def __init__(self, conf):
		JobManager.__init__(self, "saga", conf)

		self._remote_path = self._conf.get("remote_path", self._work_path)

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

	def _task_conf(self, job, key, context=None, default=None):
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

		url = self._conf.get("url", "fork://localhost", dtype=str)
		self._job_service = saga.job.Service(url, session=self._session)

		# TODO count the number of previously queued jobs

		self._running = True
		self._run_thread = threading.Thread(target=self._run_handler, name="{}-run".format(self._name))
		self._join_thread = threading.Thread(target=self._join_handler, name="{}-join".format(self._name))
		self._run_thread.start()
		self._join_thread.start()

	def _run_handler(self):

		session = self._create_session()

		while self._running:

			if self._max_queued != 0 and self._queued_count >= self._max_queued:
				time.sleep(1)
				continue

			job = None
			try:
				with self._qlock:
					job = session.query(SagaJob).filter(SagaJob.state==runstates.WAITING, SagaJob.saga_state==None)\
						.order_by(SagaJob.priority).first()

					if job is not None:
						job.state = runstates.RUNNING
						session.commit()
			except Exception as e:
				self._log.exception(e)

			if job is None:
				time.sleep(1)
				continue

			cmd, args, env = job.cmd, job.args, job.env

			jd = saga.job.Description()
			jd.environment = env

			jd.executable = cmd
			jd.arguments = args

			jd.spmd_variation = self._task_conf(job, "pe", self._pe)
			jd.total_cpu_count = self._task_conf(job, "cpu_count", self._cpu_count)

			jd.queue = self._task_conf(job, "queue", self._queue)
			jd.project = self._task_conf(job, "project", self._project)
			jd.working_directory = self._task_conf(job, "working_directory", self._working_directory)

			jd.output = os.path.join(self._remote_path, "output", "{}.txt".format(job.task_id))
			jd.error = None

			with self._qlock:
				saga_job = self._job_service.create_job(jd)
				saga_job.run()
				self._queued_count += 1
				job.saga_id = saga_job.id
				job.saga_state = saga_job.state
				session.commit()

		session.close()

	def _join_handler(self):

		session = self._create_session()

		while self._running:

			with self._qlock:
				for job in session.query(SagaJob).filter(
								SagaJob.saga_state != None,
								~SagaJob.state.in_([runstates.FINISHED, runstates.FAILED, runstates.ABORTED])):

					saga_job = self._job_service.get_job(job.saga_id)
					job.saga_state = saga_job.state

					next_state = self.__JOB_STATE_FROM_SAGA[saga_job.state]
					if job.state == next_state:
						continue

					if saga_job.state in [saga.job.DONE, saga.job.FAILED, saga.job.CANCELED]:
						job.exit_code = saga_job.exit_code
						job.created = saga_job.created
						job.started = saga_job.started
						job.finished = saga_job.finished
						#TODO exit_message, exception_trace, execution_hosts

					if job.state != runstates.WAITING and next_state == runstates.WAITING:
						self._queued_count += 1
					elif job.state == runstates.WAITING and next_state != runstates.WAITING:
						self._queued_count -= 1

					job.state = next_state
					session.commit()

					if not self._running:
						break

			time.sleep(self._state_check_interval)

		session.close()

	def _join(self, session, job):
		raise Exception("Unimplemented")

	def _output(self, job):
		raise Exception("Unimplemented")

	def _close(self):

		self._log.debug("Closing threads ...")

		self._running = False
		while self._run_thread.isAlive():
			self._run_thread.join(1)
		while self._join_thread.isAlive():
			self._join_thread.join(1)

		self._log.debug("Closing session ...")

		if self._job_service is not None:
			self._job_service.close()
			self._job_service = None

		if self._session is not None:
			self._session = None