import os
import time
import tempfile
import signal
import subprocess
import multiprocessing as mp
from threading import Thread, Lock, Condition, current_thread
from datetime import datetime

from sqlalchemy import Column, Boolean, String

from wok.core import exit_codes, runstates
from wok.core.utils.timeout import ProgressiveTimeout
from wok.core.utils.proctitle import set_thread_title

from wok.jobs.jobs import Job, JobManager

class McoreJob(Job):
	output_file = Column(String)

class McoreJobManager(JobManager):
	"""
	Multi-Core implementation of a job manager. It runs jobs in parallel using multiple cores.

	TODO: Understand and manage task resources (cpu, mem)

	Configuration::
	    **max_cores**: Maximum number of cores to use. By default it uses all the available cores.
	"""

	job_class = McoreJob

	def __init__(self, conf):
		JobManager.__init__(self, "mcore", conf)

		self._num_cores = self._conf.get("max_cores", mp.cpu_count(), dtype=int)

		self._running = False
		self._kill_threads = False
		self._threads = []

		self._run_lock = Lock()
		self._run_cvar = Condition(self._run_lock)

		self._jobs = {}

		#self._waiting_queue = PriorityQueue()

		self._task_output_files = {}

		# get hostname
		try:
			import socket
			self.hostname = socket.gethostname()
		except:
			self.hostname = "unknown"

	def _next_job(self, session):
		timeout = ProgressiveTimeout(0.5, 6.0, 0.5)
		job = None
		while job is None and self._running:
			try:
				with self._lock:
					job = session.query(McoreJob).filter(McoreJob.state == runstates.WAITING)\
													.order_by(McoreJob.priority).first()
					if job is not None:
						job.state = runstates.RUNNING
						session.commit()
			except Exception as e:
				self._log.exception(e)
				job = None
			if job is None and self._running:
				timeout.sleep() #TODO Guess how to use a Condition (self._run_cvar) or an Event

		return job

	def _update_job(self, session, job):
		session.commit()
		#with self._run_lock:
			#self._run_cvar.notify()
		self._fire_job_updated(job)

	def _run(self):
		self._log.debug("Run thread ready")

		session = self._create_session()
		try:
			while self._running:
				set_thread_title()

				job = self._next_job(session)
				if job is None:
					break

				set_thread_title("{}".format(job.name))

				self._log.debug("Running task [{}] {} ...".format(job.id, job.name))

				# Prepare execution

				script, env = job.script, job.env

				sb = ["Script:\n", script]
				if len(env) > 0:
					for k, v in env.iteritems():
						sb += ["\n{}={}".format(k, v)]
				self._log.debug("".join(sb))

				o = open(job.output, "w")

				cwd = self._conf.get("working_directory")
				if cwd is not None:
					cwd = os.path.abspath(cwd)

				job.hostname = self.hostname
				job.started = datetime.now()
				self._update_job(session, job)

				# Prepare the script file

				fd, script_path = tempfile.mkstemp(prefix=job.name + "-", suffix=".sh")
				with os.fdopen(fd, "w") as f:
					f.write(script)

				# Run the script

				exception_trace = None
				try:
					process = subprocess.Popen(
										args=["/bin/bash", script_path],
										stdin=None,
										stdout=o,
										stderr=subprocess.STDOUT,
										cwd=cwd,
										env=env,
										preexec_fn=os.setsid)

					set_thread_title("{} PID={}".format(job.name, process.pid))

					session.refresh(job, ["state"])
					timeout = ProgressiveTimeout(0.5, 6.0, 0.5)
					while job.state == runstates.RUNNING and process.poll() is None and not self._kill_threads:
						timeout.sleep()
						session.refresh(job, ["state"])

					job.finished = datetime.now()

					if process.poll() is None:
						self._log.info("Killing job [{}] {} ...".format(job.id, job.name))
						os.killpg(process.pid, signal.SIGTERM)
						timeout = ProgressiveTimeout(0.5, 6.0, 0.5)
						while process.poll() is None:
							timeout.sleep()

						job.state = runstates.ABORTED
						job.exitcode = process.returncode

					elif job.state == runstates.ABORTING:
						job.state = runstates.ABORTED

					else:
						job.state = runstates.FINISHED if process.returncode == 0 else runstates.FAILED
						job.exitcode = process.returncode

				except Exception as e:
					self._log.exception(e)
					job.state = runstates.FAILED
					job.finished = datetime.now()
					job.exitcode = exit_codes.RUN_THREAD_EXCEPTION
				finally:
					try:
						if process.poll() is None:
							self._log.info("Killing job [{}] {} ...".format(job.id, job.name))
							os.killpg(process.pid, signal.SIGKILL)
						process.wait()
					except:
						self._log.exception("Exception while waiting for process {} to finish".format(process.pid))

					o.close()
					if os.path.exists(script_path):
						os.remove(script_path)

				if job.state == runstates.FINISHED:
					self._log.debug("Job finished [{}] {}".format(job.id, job.name))
				elif job.state == runstates.ABORTED:
					self._log.debug("Job aborted [{}] {}".format(job.id, job.name))
				else:
					self._log.debug("Job failed [{}] {}".format(job.id, job.name))

				self._update_job(session, job)

		except:
			self._log.exception("Unexpected exception in thread {}".format(current_thread().name))
		finally:
			session.close()

		self._log.debug("Run thread finished")

	def _start(self):
		self._running = True

		self._log.debug("num_cores={}".format(self._num_cores))

		for i in xrange(self._num_cores):
			thread = Thread(target=self._run, name="{}-{:02}".format(self._name, i))
			self._threads.append(thread)
			thread.start()

	#TODO def _attach(self, session, job):

	def _submit(self, job):
		default_output_path = os.path.join(self._work_path, "output", job.case_name)
		output_path = self._conf.get("output_path", default_output_path)
		job.output = os.path.abspath(os.path.join(output_path, "{}.txt".format(job.name)))

		with self._run_lock:
			if not os.path.exists(output_path):
				os.makedirs(output_path)

	def _output(self, job):
		if job.output is None:
			return None

		try:
			return open(job.output)
		except:
			return None

	def _join(self, session, job):
		while self._running and job.state not in runstates.TERMINAL_STATES:
			self._run_cvar.wait(2)
			session.expire(job, [McoreJob.state])

	def _close(self):
		self._running = False
		self._kill_threads = False
		with self._run_lock:
			self._run_cvar.notify(self._num_cores)

		for thread in self._threads:
			timeout = 30
			while thread.isAlive():
				thread.join(1)
				timeout -= 1
				if timeout == 0:
					self._kill_threads = True
