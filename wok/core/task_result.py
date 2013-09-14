from datetime import datetime

_DT_FORMAT = "%Y-%m-%d %H:%M:%S"

class TaskResult(object):

	@staticmethod
	def from_native(d):
		start_time = d.get("start_time")
		if start_time is not None:
			start_time = datetime.strptime(start_time, _DT_FORMAT)

		end_time = d.get("end_time")
		if end_time is not None:
			end_time = datetime.strptime(end_time, _DT_FORMAT)

		return TaskResult(
			hostname=d.get("hostname"),
			start_time=start_time,
			end_time=end_time,
			exitcode=d.get("exitcode"),
			aborted=d.get("aborted"),
			exception=d.get("exception"),
			trace=d.get("trace"))

	def __init__(self, hostname=None, start_time=None, end_time=None, exitcode=None,
				 aborted=None, exception=None, trace=None):

		self.hostname = hostname
		self.start_time = start_time
		self.end_time = end_time
		self.exitcode = exitcode
		self.aborted = aborted
		self.exception = exception
		self.trace = trace

	def to_native(self):
		return dict(
			hostname=self.hostname,
			start_time=self.start_time.strftime(_DT_FORMAT),
			end_time=self.end_time.strftime(_DT_FORMAT),
			exitcode=self.exitcode,
			aborted=self.aborted,
			exception=self.exception,
			trace=self.trace)

	def __repr__(self):
		sb = ["TaskResult("]
		asb = []
		for attr in ["hostname", "start_time", "end_time", "exitcode", "aborted", "exception"]:
			value = getattr(self, attr)
			if value is not None:
				asb += ["{}={}".format(attr, value)]
		sb += [", ".join(asb)]
		sb += [")"]