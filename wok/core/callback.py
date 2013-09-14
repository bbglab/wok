class InvalidEvent(Exception):
	def __init__(self, event):
		Exception.__init__(self, event)

class CallbackManager(object):
	def __init__(self, valid_events=None, delegates=None):
		"""
		Manages callback events
		:param valid_events: list of valid events (optional)
		:param delegates: Other managers to delegate the event callbacks (optional).
		                  It can be a dictionary {event : callback_manager}
		                  or a list of tuples (list of events, callback_manager)
		"""

		self._callbacks = {}

		self._valid_events = set()
		if valid_events is not None:
			self._valid_events.update(valid_events)

		self._delegates = None
		if delegates is not None:
			if isinstance(delegates, dict):
				self._delegates = delegates
				self._valid_events.update(delegates.keys())
			elif isinstance(delegates, list):
				self._delegates = {}
				for delegate in delegates:
					events, callbacks = delegate
					if not isinstance(events, (tuple, list)):
						events = (events, )
					for event in events:
						self._valid_events.add(event)
						self._delegates[event] = callbacks

	@property
	def valid_events(self):
		return self._valid_events

	def add(self, event, callback):
		if self._valid_events is not None and event not in self._valid_events:
			raise InvalidEvent(event)

		if self._delegates is not None and event in self._delegates:
			self._delegates[event].add(event, callback)
			return

		if event not in self._callbacks:
			self._callbacks[event] = set()

		self._callbacks[event].add(callback)

	def remove(self, event, callback):
		if self._delegates is not None and event in self._delegates:
			self._delegates[event].remove(event, callback)
			return

		if event in self._callbacks:
			self._callbacks[event].remove(callback)

	def fire(self, event, **kargs):
		if self._delegates is not None and event in self._delegates:
			self._delegates[event].fire(event, callback)
			return

		if event in self._callbacks:
			for callback in self._callbacks[event]:
				callback(event, **kargs)