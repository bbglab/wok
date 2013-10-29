from datetime import datetime

from sqlalchemy import Column, Integer, Float, String, Text, DateTime

from wok.core import runstates

# Engine and Session ---------------------------------------

import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

#Session = scoped_session(sessionmaker())

def create_engine(uri):
	engine = sqlalchemy.create_engine(uri, connect_args=dict(timeout=1800, check_same_thread=False))
	#Session.configure(bind=engine)
	Base.metadata.create_all(engine)
	return engine

def create_session_factory(engine):
	session_factory = scoped_session(sessionmaker())
	session_factory.configure(bind=engine)
	return session_factory

# Model ------------------------------------------------------

from wok.core.db.customtypes import JSON, Config, RunState, ExitCode

class Job(Base):
	__tablename__ = "jobs"
	__table_args__ = {'sqlite_autoincrement': True}

	id = Column(Integer, primary_key=True)
	case_name = Column(String)
	name = Column(String)
	task_conf = Column(Config)
	priority = Column(Float)
	state = Column(RunState, index=True)
	created = Column(DateTime)
	started = Column(DateTime)
	finished = Column(DateTime)
	hostname = Column(String)
	exitcode = Column(ExitCode)
	output = Column(String)
	script = Column(Text)
	env = Column(JSON)

	def __init__(self, js):
		self.case_name = js.case.name
		self.name = js.job_name
		self.task_conf = js.task_conf
		self.priority = js.priority
		self.state = runstates.WAITING
		self.created = datetime.now()
		self.script = js.script
		self.env = js.env

	def __repr__(self):
		sb = ["{}(id={},case_name={},job_name={},state={}".format(
			self.__class__.__name__, self.id, self.case_name, self.name, self.state)]
		if self.created is not None:
			sb += [",created={}".format(self.created)]
		if self.started is not None:
			sb += [",started={}".format(self.started)]
		if self.finished is not None:
			sb += [",finished={}".format(self.finished)]
		if self.hostname is not None:
			sb += [",hostname={}".format(self.hostname)]
		if self.exit_code is not None:
			sb += [",exit_code={}".format(self.exit_code)]
		if hasattr(self, "_repr"):
			self._repr(sb)
		return "".join(sb + [")"])

class JobSubmission(object):
	def __init__(self, case=None, task=None, workitem_id=None, job_name=None,
				 task_conf=None, priority=None, script=None, env=None):

		self.case = case
		self.task = task
		self.workitem_id = workitem_id
		self.job_name = job_name
		self.task_conf = task_conf
		self.priority = priority
		self.script = script
		self.env = env

from collections import namedtuple

__JobResults = namedtuple("JobResults", "state, created, started, finished, exitcode, hostname")

class JobResults(__JobResults):
	def __new__(cls, job=None):
		if job is not None:
			return super(JobResults, cls).__new__(cls,
						job.state, job.created, job.started, job.finished,
						job.exitcode, job.hostname)
		else:
			return super(JobResults, cls).__new__(cls,
												  state=None, created=None, started=None, finished=None,
												  exitcode=None, hostname=None)
