import time
from threading import Lock, Condition

class AtomicCounter(object):
	def __init__(self, initial_value=0):
		self._lock = Lock()
		self._cvar = Condition(self._lock)
		self._value = initial_value

	def __iadd__(self, other):
		with self._lock:
			self._value += other
			self._cvar.notify_all()
		return self

	def __isub__(self, other):
		with self._lock:
			self._value -= other
			self._cvar.notify_all()
		return self

	def __call__(self, *args, **kwargs):
		with self._lock:
			return self._value

	def set(self, value):
		with self._lock:
			self._value = value
			self._cvar.notify_all()

	@property
	def value(self):
		with self._lock:
			return self._value

	def wait_condition(self, condition, timeout=None, check_interval=5.0):
		"""
		Waits for a given condition or a timeout.
		:param condition: A callable that will receive the counter value as a parameter.
		                  It should return True if the condition to stop waiting occurs and False otherwise.
		:param timeout: If the timeout is different than None do not wait more that this number of seconds.
		:param check_interval: Maximum time to wait between condition checks.
		:return: True if the condition was satisfied, False when there was a timeout before.
		"""
		start_time = time.time()
		with self._lock:
			while timeout is None or (time.time() - start_time) < timeout:
				if condition(self._value):
					return True
				self._cvar.wait(check_interval)
		return False

	def __repr__(self):
		with self._lock:
			return str(self._value)