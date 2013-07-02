import os.path
import threading
from datetime import datetime

from wok import logger
from wok.core import runstates
from wok.core import events
from wok.core.errors import ConfigMissingError
from wok.core.callback import CallbackManager
from wok.jobs.db import create_db, Session, Job, JobResults

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

		self._log = logger.get_logger(name="wok.jobs.{}".format(name), conf=conf.get("log"))

		if "work_path" not in self._conf:
			raise ConfigMissingError("work_path")

		self._work_path = conf["work_path"]
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		db_path = os.path.join(self._work_path, "jobs.db")
		self._db = create_db("sqlite:///{}".format(db_path))

		self._callbacks = CallbackManager(valid_events=[events.JOB_UPDATE])

		# Lock for database jobs state changes
		self._qlock = threading.Lock()

	def _create_session(self):
		return Session()

	@property
	def callbacks(self):
		return self._callbacks

	def _fire_job_updated(self, job):
		self._callbacks.fire(events.JOB_UPDATE, job_id=job.id, task_id=job.task_id, state=job.state)

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

	def start(self):
		"Start the job manager."

		self._log.info("Starting '{}' job manager ...".format(self._name))

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

		session = self._create_session()
		try:
			for js in submissions:
				job = self.job_class(js)
				self._submit(job)
				session.add(job)
				session.commit()
				yield js, job.id
		finally:
			session.close()

	def state(self, job_ids=None):
		"""
		Returns an iterator of (job_id, state) for all the jobs in jobs_id. If job_ids is None then all the jobs are queried.
		:param job_ids: list of job ids or None
		:return: Iterator of (job_id, state)
		"""

		session = self._create_session()
		query = session.query(self.job_class.id, self.job_class.state)
		if job_ids is not None:
			query = query.filter(self.job_class.state.in_(job_ids))
		for job_id, job_state in query:
			yield job_id, job_state
		session.close()

	def output(self, job_id):
		"Get the output of the job"

		job = self._get_job(job_id)
		if job is None:
			return None

		self._log.debug("Retrieving output for job [{}] {} ...".format(job.id, job.task_id))

		return self._output(job)

	def job(self, job_id):
		"""Returns a job by its id. A job can only be retrieved while
		it has not been joined. Once joined the job structure is deleted."""

		raise Exception("Abstract method unimplemented")

	def join(self, job_id):
		"Retrieve the job results and delete the job from memory."

		session = self._create_session()
		job = session.query(self.job_class).filter(self.job_class.id == job_id).first()

		if job is None:
			session.close()
			return

		self._log.debug("Joining job [{}] {} ...".format(job.id, job.task_id))

		self._join(session, job)

		session.delete(job)
		session.commit()
		session.close()

		return JobResults(job)

	def _abort(self, session, job):
		pass

	def abort(self, job_ids=None):
		"Aborts a job"

		with self._qlock:
			session = self._create_session()

			job = session.query(self.job_class).filter(self.job_class.id == job_id).first()

			if job is None:
				session.close()
				return

			self._log.debug("Aborting job [{}] {} ...".format(job.id, job.task_id))

			job.state = runstates.ABORTING
			session.commit()

		self._abort(session, job)

		session.close()

	def close(self):
		"Close the job manager and free resources."

		self._log.info("Closing '{}' job manager ...".format(self._name))

		session = self._create_session()

		with self._qlock:
			session.query(self.job_class).filter(self.job_class.state == runstates.WAITING)\
				.update({self.job_class.state : runstates.ABORTED}, synchronize_session=False)
			session.commit()

		self._close()

		session.query(self.job_class).delete() # FIXME remove this line when engine saves state
		session.commit()
		session.close()

		Session.remove()

		self._log.info("Job manager '{}' closed".format(self._name))