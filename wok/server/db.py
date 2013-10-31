#from flask.ext.sqlalchemy import SQLAlchemy
#db = SQLAlchemy()

import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

Session = scoped_session(sessionmaker(autocommit=False, autoflush=False))

def create_engine(uri, drop_tables=False):
	engine = sqlalchemy.create_engine(uri, convert_unicode=True,
						   connect_args=dict(timeout=6, check_same_thread=False))
	Session.configure(bind=engine)
	Base.query = Session.query_property()
	if drop_tables:
		Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)
	return engine

