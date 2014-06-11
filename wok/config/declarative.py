from data import Data

class UndefinedConfigParam(Exception):
	pass

def _indent(sb):
	from StringIO import StringIO
	return "\n".join(["  " + s.rstrip("\n") for s in StringIO(sb)])

class Param(object):
	def __init__(self, default=None, key=None, type=str, required=False):
		self.key = key
		self.type = type
		self.default = default
		self.required = required
		self.conf = None
		self.namespace = []

	def validate(self):
		if self.required and self.key not in self.conf:
			raise UndefinedConfigParam(".".join(self.namespace + [self.key]))

	def get(self):
		return self.conf.get(key=self.key, dtype=self.type, default=self.default)

	def __repr__(self):
		return "{} : {} (default={}, type={}, req={}, key={}) = {}".format(
			self.key, self.__class__.__name__, self.default,
			self.type.__name__ if self.type is not None else None,
			self.required, ".".join(self.namespace + [self.key]), self.get())

class BoolParam(Param):
	def __init__(self, default=None, key=None, required=False):
		super(BoolParam, self).__init__(default=default, key=key, type=bool, required=required)

class IntParam(Param):
	def __init__(self, default=None, key=None, required=False):
		super(IntParam, self).__init__(default=default, key=key, type=int, required=required)

class FloatParam(Param):
	def __init__(self, default=None, key=None, required=False):
		super(FloatParam, self).__init__(default=default, key=key, type=float, required=required)

class StringParam(Param):
	def __init__(self, default=None, key=None, required=False):
		super(StringParam, self).__init__(default=default, key=key, type=str, required=required)

class ListParam(Param):
	def __init__(self, type, default=None, key=None, required=False):
		super(ListParam, self).__init__(default=default, key=key, type=type, required=required)

	def get(self):
		conf = self.conf.get(key=self.key, dtype=Data.list, default=Data.list)
		return [self.type(e) for e in conf]

class DataParam(Param):
	def __init__(self, default=None, key=None, required=False):
		super(DataParam, self).__init__(default=default, key=key, type=None, required=required)

	def get(self):
		return self.conf.get(key=self.key, default=self.default)

class Config(object):

	def __init__(self, *confs, **kwargs):
		conf = Data.element()
		for c in confs:
			conf.merge(Data.element(c))

		self._initialized = False

		self.key = kwargs.get("key")
		self.namespace = kwargs.get("namespace") or []

		if len(confs) > 0:
			self._init_params(conf)

	def _params(self):
		for param_name in dir(self.__class__):
			param = getattr(self.__class__, param_name)
			if isinstance(param, (Param, Config)):
				yield param_name, param

	def _init_params(self, conf):
		self._conf = conf
		for param_name in dir(self.__class__):
			param = getattr(self, param_name)
			cparam = getattr(self.__class__, param_name)
			if isinstance(param, Config):
				cparam.key = cparam.key or param_name
				param = param.__class__(key=param.key or param_name)
				setattr(self, param_name, param)
				param.namespace = self.namespace + [param.key]
				param._init_params(conf.get(param.key, default=Data.element))
				param.validate()
			elif isinstance(param, Param):
				if param.key is None:
					param.key = param_name
				param.namespace = self.namespace
				param.conf = self._conf
				param.validate()
				setattr(self, param_name, param.get())

	def validate(self):
		pass

	def __repr__(self):
		sb = []
		for param_name in dir(self.__class__):
			param = getattr(self, param_name)
			cparam = getattr(self.__class__, param_name)
			if isinstance(cparam, Config):
				sb += [repr(param)]
			elif isinstance(cparam, Param):
				sb += [repr(cparam)]

		if self.key is None:
			decl = self.__class__.__name__
		else:
			decl = "{} : {}".format(self.key, self.__class__.__name__)

		return "\n".join([decl, _indent("\n".join(sb))])
