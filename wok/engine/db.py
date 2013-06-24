from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

Session = sessionmaker()

def create_db(uri):
	engine = create_engine(uri)
	Session.configure(bind=engine)
	return engine