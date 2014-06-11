import os.path
import threading
from datetime import datetime

from wok import logger
from wok.core import runstates
from wok.core import events
from wok.core.errors import MissingConfigParamError
from wok.core.callback import CallbackManager
from wok.jobs.db import create_engine, create_session_factory, Job, JobResults

import inspect
# Class to wrap Lock and simplify logging of lock usage
class LogLock(object):
	"""
	Wraps a standard Lock, so that attempts to use the
	lock according to its API are logged for debugging purposes

	"""
	def __init__(self, name, log):
		self.name = str(name)
		self.log = log
		self.lock = threading.Lock()
		self.log.debug("<CREATED {} {}>".format(self.name, inspect.stack()[2][3]))

	def acquire(self, blocking=True, l=1):
		caller = inspect.stack()[l][3]
		self.log.debug("<TRYING {} {}>".format(self.name, caller))
		ret = self.lock.acquire(blocking)
		if ret == True:
			self.log.debug("<ACQUIRED {} {}>".format(self.name, caller))
		else:
			self.log.debug("<FAILED {} {}>".format(self.name, caller))
		return ret

	def release(self, l=1):
		caller = inspect.stack()[l][3]
		self.log.debug("<RELEASING {} {}>".format(self.name, caller))
		self.lock.release()

	def __enter__(self):
		self.acquire(l=2)

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.release(l=2)
		return False # Do not swallow exceptions

class JobManager(object):
	"""
	Base class for Job managers. It implements the main interface (start, submit, join, ...)
	and manages de jobs database, then it delegates core operations to the child class (_start, _submit, ...)

	Configuration namespace: **wok.jobs**
	    **work_path**: Working path for temporary files and databases.
	"""

	job_class = Job

	def __init__(self, name, conf):
		self._name = name
		self._conf = conf

		self._log = logger.get_logger("wok.jobs.{}".format(name))

		if "work_path" not in self._conf:
			raise MissingConfigParamError("work_path")

		self._work_path = conf["work_path"]
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		db_path = os.path.join(self._work_path, "jobs.db")
		# TODO if not in recover_mode then delete jobs.db
		self._db = create_engine("sqlite:///{}".format(db_path))
		self._session_factory = create_session_factory(self._db)

		self._callbacks = CallbackManager(valid_events=[events.JOB_UPDATE])

		# Lock for database jobs state changes
		self._lock = threading.Lock()
		#self._lock = LogLock("lock", self._log)

	def _create_session(self):
		return self._session_factory()

	@property
	def callbacks(self):
		return self._callbacks

	def _fire_job_updated(self, job):
		self._callbacks.fire(events.JOB_UPDATE, job_id=job.id, workitem_cname=job.name, state=job.state)

	def _get_job(self, job_id):
		session = self._create_session()
		job = session.query(self.job_class).filter(self.job_class.id == job_id).first()
		session.close()
		return job

	def _task_conf(self, job, key, context=None, default=None):
		task_conf = job.task_conf
		if context is not None:
			key = "{}.{}".format(context, key)
		return task_conf.get(key, default)

	def add_callback(self, event, callback):
		"""
		Adds an event callback. For example whenever there are job state changes it will be called.
		:param callback: callee accepting the following parameters: event, **kargs
		:return:
		"""
		self._callbacks[event].add(callback)

	def _attach(self, session, job):
		session.delete(job)

	def start(self):
		"Start the job manager."

		self._log.info("Starting '{}' job manager ...".format(self._name))

		with self._lock:
			session = self._create_session()
			for job in session.query(self.job_class):
				self._attach(session, job)
				session.commit()
			session.close()

		self._start()

		self._log.info("Job manager '{}' started ...".format(self._name))

	def _submit(self, job):
		pass

	def submit(self, submissions):
		"""
		Submit jobs.
		:param submissions: list or iterator with the job submissions.
		:return: Iterator of (job_submission, job_id) for all the tasks.
		"""

		with self._lock:
			session = self._create_session()
			try:
				for js in submissions:
					job = self.job_class(js)
					self._submit(job)
					session.add(job)
					session.commit()
					self._lock.release()
					yield js, job.id, job.state
					self._lock.acquire()
			finally:
				session.close()

	def state(self, job_ids=None):
		"""
		Returns an iterator of (job_id, state) for all the jobs in jobs_id. If job_ids is None then all the jobs are queried.
		:param job_ids: list of job ids or None
		:return: Iterator of (job_id, state)
		"""

		with self._lock:
			session = self._create_session()
			query = session.query(self.job_class.id, self.job_class.state)
			if job_ids is not None:
				query = query.filter(self.job_class.state.in_(job_ids))
			for job_id, job_state in query:
				self._lock.release()
				yield job_id, job_state
				self._lock.acquire()
			session.close()

	def output(self, job_id):
		"Get the output of the job"

		with self._lock:
			job = self._get_job(job_id)
			if job is None:
				return None

			self._log.debug("Retrieving output for job [{}] {} ...".format(job.id, job.name))

			return self._output(job)

	def join(self, job_id):
		"Retrieve the job results and delete the job from memory."

		with self._lock:
			session = self._create_session()
			job = session.query(self.job_class).filter(self.job_class.id == job_id).first()

			if job is None:
				session.close()
				return JobResults()

			self._log.debug("Joining job [{}] {} ...".format(job.id, job.name))

			self._join(session, job)

			session.delete(job)
			session.commit()
			session.close()

		return JobResults(job)

	def _abort(self, session, job):
		pass

	def abort(self, job_ids=None):
		"Aborts a job"

		with self._lock:
			session = self._create_session()

			abortable_states = {
					runstates.WAITING : runstates.ABORTED,
					runstates.RUNNING : runstates.ABORTING }

			for job_id in job_ids:
				job = session.query(self.job_class).filter(self.job_class.id == job_id).first()

				if job is None or job.state not in abortable_states:
					continue

				job.state = abortable_states[job.state]

				self._log.debug("{} job [{}] {} ...".format(job.state.title.capitalize(), job.id, job.name))

				self._abort(session, job)

			session.commit()
			session.close()

	def close(self):
		"Close the job manager and free resources."

		self._log.info("Closing '{}' job manager ...".format(self._name))

		with self._lock:
			session = self._create_session()

			session.query(self.job_class).filter(
				self.job_class.state == runstates.WAITING).update(
				{self.job_class.state : runstates.ABORTED}, synchronize_session=False)
			session.commit()

			self._close()

		session.close()

		self._session_factory.remove()

		self._log.info("Job manager '{}' closed".format(self._name))