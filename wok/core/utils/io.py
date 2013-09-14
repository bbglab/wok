import os
import fcntl

def touchopen(filename, *args, **kwargs):
	# Open the file in R/W and create if it doesn't exist. *Don't* pass O_TRUNC
	fd = os.open(filename, os.O_RDWR | os.O_CREAT)
	# Encapsulate the low-level file descriptor in a python file object
	return os.fdopen(fd, *args, **kwargs)

class FileLock(object):
	lock_type = fcntl.LOCK_EX

	def __init__(self, filename, *args, **kwargs):
		self.filename = filename
		self.args = args
		self.kwargs = kwargs

	def __enter__(self):
		self.f = touchopen(self.filename, *self.args, **self.kwargs)
		fcntl.lockf(self.f, self.lock_type)
		return self.f

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.f.close()

class FileLockEx(FileLock):
	lock_type = fcntl.LOCK_EX