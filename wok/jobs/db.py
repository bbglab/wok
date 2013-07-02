from datetime import datetime

from sqlalchemy import Column, Integer, Float, String, Text, DateTime

# Engine and Session ---------------------------------------

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

Session = scoped_session(sessionmaker())

def create_db(uri):
	engine = create_engine(uri)
	Session.configure(bind=engine)
	Base.metadata.create_all(engine)
	return engine

# Custom types ---------------------------------------------

import json

import sqlalchemy.types as types

from wok.config.data import Data
from wok.core import runstates
from wok.core import exit_codes

class JSON(types.TypeDecorator):

	impl = types.Text

	def process_bind_param(self, value, dialect):
		return json.dumps(value)

	def process_result_value(self, value, dialect):
		return json.loads(value)

	def copy(self):
		return JSON(self.impl.length)

class Config(types.TypeDecorator):

	impl = types.Text

	def process_bind_param(self, value, dialect):
		return json.dumps(value.to_native())

	def process_result_value(self, value, dialect):
		return Data.create(json.loads(value))

	def copy(self):
		return Config(self.impl.length)

class RunState(types.TypeDecorator):

	impl = types.Integer

	def process_bind_param(self, value, dialect):
		return value.id

	def process_result_value(self, value, dialect):
		return runstates.from_id(value)


class ExitCode(types.TypeDecorator):

	impl = types.Integer

	def process_bind_param(self, value, dialect):
		return value.code if value is not None and isinstance(value, exit_codes.ExitCode) else value

	def process_result_value(self, value, dialect):
		return exit_codes.ExitCode.from_code(value) if value is not None else None

# Model ------------------------------------------------------

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Job(Base):
	__tablename__ = "jobs"

	id = Column(Integer, primary_key=True)
	instance_id = Column(String)
	task_id = Column(String)
	task_conf = Column(Config)
	priority = Column(Float)
	state = Column(RunState, index=True)
	created = Column(DateTime)
	started = Column(DateTime)
	finished = Column(DateTime)
	hosts = Column(String)
	output = Column(String)
	script = Column(Text)
	env = Column(JSON)
	exit_code = Column(ExitCode)
	exit_message = Column(Text)
	exception_trace = Column(Text)

	def __init__(self, js):
		self.instance_id = js.instance_id
		self.task_id = js.task_id
		self.task_conf = js.task_conf
		self.priority = js.priority
		self.state = runstates.WAITING
		self.created = datetime.now()
		self.script = js.script
		self.env = js.env

	def __repr__(self):
		sb = ["{}(id={},instance_id={},task_id={},state={}".format(self.__class__.__name__,
																   self.id, self.instance_id, self.task_id, self.state)]
		if self.created is not None:
			sb += [",created={}".format(self.created)]
		if self.started is not None:
			sb += [",started={}".format(self.started)]
		if self.finished is not None:
			sb += [",finished={}".format(self.finished)]
		if self.hosts is not None:
			sb += [",hosts={}".format(self.hosts)]
		if self.exit_code is not None:
			sb += [",exit_code={}".format(self.exit_code)]
		if self.exit_message is not None:
			sb += [",exit_message={}".format(self.exit_message)]
		if hasattr(self, "_repr"):
			self._repr(sb)
		return "".join(sb + [")"])

class JobSubmission(object):
	def __init__(self, task=None, instance_id=None, task_id=None, task_conf=None,
				 priority=None, script=None, env=None):
		self.task = task
		self.instance_id = instance_id
		self.task_id = task_id
		self.task_conf = task_conf
		self.priority = priority
		self.script = script
		self.env = env

from collections import namedtuple

__JobResults = namedtuple("JobResults", "state, created, started, finished, exit_code, exit_message, exception_trace")

class JobResults(__JobResults):
	def __new__(cls, job):
		return super(JobResults, cls).__new__(cls,
						job.state, job.created, job.started, job.finished,
						job.exit_code, job.exit_message, job.exception_trace)
