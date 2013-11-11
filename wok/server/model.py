from sqlalchemy import Table, Column, Text, String, Integer, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref

from flask.ext.login import UserMixin

from wok.core.db.customtypes import Config, RunState

from db import Base

DB_VERSION = 2

ADMIN_GROUP = "admin"
USERS_GROUP = "users"

user_groups_table = Table('user_groups', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('group_id', Integer, ForeignKey('groups.id'))
)

class DbParam(Base):
	__tablename__ = "__db_params"

	name = Column(String, primary_key=True)
	value = Column(String)

class Grant(object):
	id = Column(Integer, primary_key=True)
	name = Column(String)
	value = Column(String)

class UserGrant(Base, Grant):
	__tablename__ = "user_grants"

	user_id = Column(Integer, ForeignKey("users.id"))

class GroupGrant(Base, Grant):
	__tablename__ = "group_grants"

	group_id = Column(Integer, ForeignKey("groups.id"))

class User(Base, UserMixin):
	__tablename__ = "users"

	id = Column(Integer, primary_key=True)

	nick = Column(String)
	name = Column(String)
	email = Column(String)
	password = Column(String)

	active = Column(Boolean, default=True)

	groups = relationship("Group", secondary=user_groups_table, backref="users")

	grants = relationship("UserGrant", backref="user")

	cases = relationship("Case", backref="owner")

	def is_active(self):
		return self.active

	def __repr__(self):
		sb = []
		if self.name is not None:
			sb += [self.name]
		if self.email is not None:
			sb += ["<{}>".format(self.email)]
		return " ".join(sb)

class Group(Base):
	__tablename__ = "groups"

	id = Column(Integer, primary_key=True)

	name = Column(String, unique=True)
	desc = Column(String)

	grants = relationship("GroupGrant", backref="group")

class Case(Base):
	__tablename__ = "cases"
	__table_args__ = {'sqlite_autoincrement': True}

	id = Column(Integer, primary_key=True)

	owner_id = Column(Integer, ForeignKey("users.id"))

	name = Column(String)
	created = Column(DateTime)
	started = Column(DateTime)
	finished = Column(DateTime)
	state = Column(RunState)
	removed = Column(Boolean, default=False)
	engine_name = Column(String, index=True, unique=True)
	project_name = Column(String)
	flow_name = Column(String)
	conf = Column(Config)
	properties = Column(Config)

