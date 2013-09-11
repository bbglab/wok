from sqlalchemy import Column, Text, String, Integer, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref

from flask.ext.login import UserMixin

from wok.core.db.customtypes import Config

from db import Base

class User(Base, UserMixin):
	__tablename__ = "users"

	id = Column(Integer, primary_key=True)

	nick = Column(String)
	name = Column(String)
	email = Column(String)
	password = Column(String)

	active = Column(Boolean, default=True)

	cases = relationship("Case", backref="case")

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

class Case(Base):
	__tablename__ = "cases"
	__table_args__ = {'sqlite_autoincrement': True}

	id = Column(Integer, primary_key=True)

	owner_id = Column(Integer, ForeignKey("users.id"))

	name = Column(String)
	created = Column(DateTime)
	engine_name = Column(String)
	flow_uri = Column(String)
	conf = Column(Config)
	properties = Column(Config)

