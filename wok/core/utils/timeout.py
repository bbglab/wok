import time

class ProgressiveTimeout(object):
	def __init__(self, start=0.5, maximum=6.0, step=0.5):
		self.start = start
		self.timeout = start
		self.maximum = maximum
		self.step = step

	def next(self):
		timeout = self.timeout
		self.timeout += self.step
		self.timeout = min(self.maximum, self.timeout)
		return timeout

	def reset(self):
		self.timeout = self.start

	def sleep(self):
		time.sleep(self.next())