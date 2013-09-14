import os

from uuid import uuid4
from threading import Lock

from sqlalchemy import inspect
from blinker import Signal

from flask import Flask, redirect, url_for, current_app, session

from flask.ext.login import LoginManager

#from flask.ext.login import (LoginManager, current_user, login_required,
#							login_user, logout_user, UserMixin, AnonymousUser,
#							confirm_login, fresh_login_required)

from wok import logger
from wok.config.data import Data
from wok.engine import WokEngine

import db
from model import User, Group, Case, USERS_GROUP

import core

class WokFlask(Flask):
	def __init__(self, *args, **kwargs):
		if not (len(args) > 0 or "import_name" in kwargs):
			#kwargs["import_name"] = __name__
			args += (__name__,)

		Flask.__init__(self, *args, **kwargs)


class WokServer(object):

	# signals

	case_removed = Signal()


	def __init__(self):
		self.flask = None
		self.engine = None
		self.lock = Lock()

		self._initialized = False

		self._work_path = None

		self._endpoints = {} # TODO remove

		self.logger = logger.get_logger("wok.server")

	def _generate_secret_key(self):
		path = os.path.join(self._work_path, "secret_key.bin")

		if not os.path.exists(path):
			self.logger.info("Generating a new secret key ...")
			secret_key = os.urandom(24)
			with open(path, "wb") as f:
				f.write(secret_key)
		else:
			self.logger.info("Loading secret key ...")

		with open(path, "rb") as f:
			secret_key = f.read()
		if len(secret_key) < 24:
			self.logger.warn("Found a secret key too short. Generating a new larger one.")
			secret_key = os.urandom(24)

		return secret_key

	def _init_flask(self):
		"""
		Initialize the Flask application. Override *_create_flask_app()* to use another class.
		"""

		self.logger.info("Initializing Flask application ...")

		if self.flask is None:
			self.flask = WokFlask(__name__)

		self.flask.wok = self

		self.flask.logger_name = "web"

		if self.flask.secret_key is None:
			self.flask.secret_key = self._generate_secret_key()

		self.flask.teardown_request(self._teardown_request)

		self.login_manager = LoginManager()
		self.login_manager.init_app(self.flask)
		self.login_manager.user_loader(self._load_user)
		self.login_manager.login_message = "Please sign in to access this page."
		#self.login_manager.anonymous_user = ...

		self.flask.register_blueprint(core.bp, url_prefix="/core")

	def _teardown_request(self, exception):
		db.Session.remove()

	def _init_engine(self):
		"""
		Initialize the Wok Engine.
		"""
		self.logger.info("Initializing Wok engine ...")

		self.engine = WokEngine(self.conf)
		self.engine.case_removed.connect(self._case_removed)
		self.engine.start(wait=False)

	def init(self, conf, app):
		self.conf = conf
		self.flask = app

		# FIXME is this really needed ? what about self.engine.work_path ?
		self._work_path = self.conf.get("wok.work_path", os.path.join(os.getcwd(), "wok-files"))
		if not os.path.exists(self._work_path):
			os.makedirs(self._work_path)

		self._init_flask()
		self._init_engine()

		db_path = os.path.join(self.engine.work_path, "server.db")
		db.create_engine(uri="sqlite:///{}".format(db_path))

		self._initialized = True

	def _finalize(self):
		self.logger.info("Finalizing Wok server ...")
		with self.lock:
			self.logger.info("Finalizing Wok engine ...")
			self.engine.stop()

	def run(self):
		if not self._initialized:
			raise Exception("The server requires initialization before running")

		server_mode = self.conf.get("wok.server.enabled", False, dtype=bool)
		server_host = self.conf.get("wok.server.host", "0.0.0.0", dtype=str)
		server_port = self.conf.get("wok.server.port", 5000, dtype=int)
		server_debug = self.conf.get("wok.server.debug", False, dtype=bool)

		try:
			self.flask.run(
					host=server_host,
					port=server_port,
					debug=server_debug,
					use_reloader=False)

			# user has pressed ctrl-C and flask stops

		finally:
			self._finalize()

	def shutdown(self):
		"""
		Shutdown the Werkzeug Server. Only valid if running under the provided Werkzeug Server. Requires Werkzeug 0.8+
		"""
		werkzeug_shutdown = request.environ.get('werkzeug.server.shutdown')
		if werkzeug_shutdown is None:
			raise RuntimeError('Not running with the Werkzeug Server')
		werkzeug_shutdown()

	# Authentication ---------------------------------------------------------------------------------------------------

	def _load_user(self, user_id):
		"LoginManager user loader."
		user = User.query.filter_by(id=user_id).first()
		#current_app.logger.debug("load_user({}) = {}".format(user_id, repr(user)))
		return user

	# Users ------------------------------------------------------------------------------------------------------------

	def register_user(self, **kwargs):
		"""
		Register a new user in the database. Either nick or email attributes is required.
		"""
		assert("nick" in kwargs or "email" in kwargs)

		session = db.Session()

		group = session.query(Group).filter(Group.name == USERS_GROUP).first()
		if group is None:
			group = Group(name=USERS_GROUP, desc="Users")
			session.add(group)

		user = User(**kwargs)
		if user.name is None:
			user.name = user.nick or user.email
		user.groups += [group]

		session.add(user)
		session.commit()
		return user

	def get_user_by_email(self, email):
		return User.query.filter_by(email=email).first()

	# Cases ------------------------------------------------------------------------------------------------------------

	def _case_removed(self, engine_case):
		session = db.Session()
		case = session.query(Case).filter(Case.engine_name == engine_case.name).first()
		if case is not None:
			self.case_removed.send(case)
			session.delete(case)
			session.commit()

	def exists_case(self, user, case_name):
		engine_case_name = "{}-{}".format(user.nick, case_name)
		exists_in_db = lambda: Case.query.filter(Case.owner_id == user.id, Case.name == case_name).count() > 0
		return self.engine.exists_case(engine_case_name) or exists_in_db()

	def create_case(self, user, case_name, conf_builder, flow_uri, properties=None, start=True):
		case = Case(
					owner_id=user.id,
					name=case_name,
					flow_uri=flow_uri,
					conf=conf_builder.get_conf(),
					properties=Data.element(properties))

		session = db.Session()
		session.add(case)
		session.commit()

		engine_case_name = "{}-{}".format(user.nick, case_name)
		#while self.engine.exists_case(engine_case_name):
		#	engine_case_name = "{}-{}".format(user.nick, uuid4().hex[-6:])

		engine_case = self.engine.create_case(engine_case_name, conf_builder, flow_uri)

		case.created = engine_case.created
		case.engine_name = engine_case_name
		session.commit()

		if start:
			engine_case.start()

		return case

	def remove_case(self, user, case):
		# The case will be remove from the db when the case remove signal arrives
		case.removed = True
		session = inspect(case).session
		session.commit() # FIXME sure to do the commit here ?

		if self.engine.exists_case(case.engine_name):
			self.engine.remove_case(case.engine_name)
		else:
			self.logger.debug("No engine case available: {}".format(case.engine_name))
			self.case_removed.send(case)
			session.delete(case)
			session.commit()

	def case_by_id(self, case_id, user=None):
		q = Case.query.filter(Case.id == case_id)
		if user is not None:
			q = q.filter(Case.owner_id == user.id)
		return q.first()

	def cases(self, user=None):
		q = Case.query
		if user is not None:
			q = q.filter(Case.owner_id == user.id)
		return q.all()