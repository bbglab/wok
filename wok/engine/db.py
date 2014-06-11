from datetime import datetime

# Engine and Session ---------------------------------------

import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

Session = scoped_session(sessionmaker())

def create_engine(uri, drop_tables=False):
	engine = sqlalchemy.create_engine(uri, connect_args=dict(timeout=18000, check_same_thread=False))
	Session.configure(bind=engine)
	if drop_tables:
		Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)
	return engine

# Model --------------------------------------------------------------

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Float, String, Text, DateTime, Boolean
from sqlalchemy.orm import relationship, backref

from wok.core.db.customtypes import Config, RunState

class Node(object):
	id = Column(Integer, primary_key=True)

	ns = Column(String)
	name = Column(String)
	cname = Column(String)

	title = Column(String)
	desc = Column(String)

	enabled = Column(Boolean, default=True)

class Case(Base):
	__tablename__ = "cases"

	id = Column(Integer, primary_key=True)

	name = Column(String, unique=True)
	title = Column(String)
	created = Column(DateTime)
	project = Column(String)
	flow = Column(String)
	storage = Column(String)
	conf = Column(Config)
	state = Column(RunState)
	removed = Column(Boolean, default=False)

	components = relationship("Component", backref="case")

class Param(Base):
	__tablename__ = "params"

	id = Column(Integer, primary_key=True)

	parent_id = Column(Integer, ForeignKey("components.id"))

	name = Column(String)
	title = Column(String)
	desc = Column(String)
	required = Column(Boolean, default=False)
	type = Column(String)
	validation = Column(String)

class Port(Node, Base):
	__tablename__ = "ports"

	parent_id = Column(Integer, ForeignKey("components.id"))

	mode = Column(String)

	wsize = Column(Integer) #TODO remove

	#links = Column()

class Component(Node, Base):
	__tablename__ = "components"
	__mapper_args__ = {"polymorphic_on": "ctype"}

	ctype = Column(String)

	case_id = Column(Integer, ForeignKey("cases.id"))

	parent_id = Column(Integer, ForeignKey("components.id"))
	parent = relationship("Component", remote_side="Component.id")

	maxpar = Column(Integer) #TODO move to stream
	wsize = Column(Integer) #TODO move to stream

	priority = Column(Float)
	priority_factor = Column(Float)

	resources = Column(Config)

	params = relationship(Param, backref="parent")

	ports = relationship(Port, backref="parent")

	state = Column(RunState)

	started = Column(DateTime)
	finished = Column(DateTime)

	#depends = Column()
	#waiting = Column()
	#notify = Column()

	conf = Column(Config)

class Block(Component, Node):
	__tablename__ = "blocks"
	__mapper_args__ = {"polymorphic_identity": "B"}

	id = Column(Integer, ForeignKey('components.id'), primary_key=True)

	library = Column(String)
	version = Column(String)

	flow_path = Column(String)

	children = relationship(Component, foreign_keys=[Component.parent_id], #backref="parent",
							remote_side=Component.id, viewonly=True)

class Task(Component, Node):
	__tablename__ = "tasks"
	__mapper_args__ = {"polymorphic_identity": "T"}

	id = Column(Integer, ForeignKey('components.id'), primary_key=True)

	type = Column(String) # source, process, sink #TODO nullable=False

	execution = Column(Config)

class WorkItem(Base):
	__tablename__ = "workitems"

	id = Column(Integer, primary_key=True)

	case_id = Column(Integer, ForeignKey(Case.id))
	case = relationship("Case")

	task_id = Column(Integer, ForeignKey(Task.id))
	task = relationship("Task")

	ns = Column(String)
	name = Column(String)
	cname = Column(String)

	index = Column(Integer)
	state = Column(RunState)
	substate = Column(RunState)

	priority = Column(Float)

	platform = Column(String)

	partition = Column(Config)

	job_id = Column(String)

	result = Column(Config)