import os

from builder import ConfigBuilder

class ConfigProcessor(object):
	def __init__(self, logger, path, args_files, args_data, req_files=None, req_keys=None):
		self.log = logger
		self.conf_path = path

		req_files = req_files or []
		req_keys = req_keys or []

		self.conf_keys = req_keys

		self.required_conf_files = [os.path.join(self.conf_path, cf) for cf in req_files]

		self.conf_files = []
		if args_files is not None:
			for cf in args_files:
				if not os.path.isabs(cf):
					cf = os.path.join(os.getcwd(), cf)
				self.conf_files += [cf]

		self.conf_data = args_data or []

		self.conf_builder = ConfigBuilder()

	def load_files(self):
		conf_files = self.required_conf_files

		user_conf_file = os.path.join(self.conf_path, "user.conf")
		if os.path.exists(user_conf_file):
			conf_files += [user_conf_file]

		conf_files += self.conf_files

		# Check that configuration files exist

		missing_conf_files = [cf for cf in conf_files if not os.path.exists(cf)]
		if len(missing_conf_files) > 0:
			self.log.error("Configuration files not found:\n{}".format(
							"\n".join("  {}".format(cf) for cf in missing_conf_files)))
			exit(-1)

		for cf in conf_files:
			self.conf_builder.add_file(cf)

		return self.conf_builder

	def parse_data(self):
		for data in self.conf_data:
			try:
				pos = data.index("=")
				key = data[0:pos]
				value = data[pos+1:]
				try:
					v = json.loads(value)
				except:
					v = value
				self.conf_builder.add_value(key, v)
			except:
				raise Exception("Wrong configuration data: KEY=VALUE expected but found '{}'".format(data))

		return self.conf_builder

	def load(self):
		self.load_files()
		self.parse_data()

	def validated_conf(self, expand_vars=True):
		self.conf = self.conf_builder.get_conf()
		if expand_vars:
			self.conf.expand_vars()

		#self.log.debug(repr(self.conf))

		# Validate that required keys exist

		mk = self.conf.missing_keys(self.conf_keys)
		if len(mk) > 0:
			sb = ["The following configuration parameters were not found:\n",
					"\n".join("* " + k for k in mk),
					"\nin any of the following configuration files:\n",
					"\n".join("* " + k for k in conf_files)]
			self.log.error("".join(sb))
			exit(-1)

		return self.conf

	def log_debug(self):
		if len(self.conf_files) > 0:
			self.log.debug("User defined configuration files:\n{}".format(
							"\n".join("  {}".format(cf) for cf in self.conf_files)))

		self.log.debug("Effective configuration: " + str(self.conf))