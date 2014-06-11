import os

from processor import ConfigProcessor

class Arguments(object):

	def __init__(self, parser,
				 group_name="Wok Options", case_name_args=True, engine_args=True, case_args=True,
				 conf_path=None,
				 engine_default_conf_files=None, engine_required_conf=None,
				 case_default_conf_files=None, case_required_conf=None,
				 logger=None):

		self.case_name_args = case_name_args
		self.engine_args = engine_args
		self.case_args = case_args

		self._add_arguments(parser, group_name, case_name_args, engine_args, case_args)

		self.conf_path = conf_path or os.getcwd()
		self.engine_default_conf_files = engine_default_conf_files
		self.engine_required_conf = engine_required_conf
		self.case_default_conf_files = case_default_conf_files
		self.case_required_conf = case_required_conf
		self.logger = logger

		self.case_name = None
		self.engine_conf_proc = None
		self.case_conf_proc = None
		
	def _add_arguments(self, parser, group_name="Wok Options", case_name_args=True, engine_args=True, case_args=True):
		g = parser.add_argument_group(group_name) if group_name is not None else parser

		if case_name_args:
			g.add_argument("-n", "--case", dest = "case_name", metavar = "NAME",
							  help = "Define the case name")

		if engine_args:
			g.add_argument("-c", "--engine-conf", action="append", dest="engine_conf_files", metavar="FILE",
						 help="Load engine configuration from a file. Multiple files can be specified")
	
			g.add_argument("-d", action="append", dest="engine_conf_data", metavar="PARAM=VALUE",
						 help="Define engine configuration parameter. Multiple definitions can be specified. Example: -d option1=value")

		if case_args:
			g.add_argument("-C", "--case-conf", action="append", dest="case_conf_files", metavar="FILE",
						 help="Load case configuration from a file. Multiple files can be specified")
	
			g.add_argument("-D", action="append", dest="case_conf_data", metavar="PARAM=VALUE",
						 help="Define case configuration parameter. Multiple definitions can be specified. Example: -D option1=value")
	
	def initialize(self, args):
		if self.case_name_args:
			self.case_name = args.case_name

		if self.engine_args:
			self.engine_conf_proc = ConfigProcessor(self.logger, self.conf_path,
													args.engine_conf_files, args.engine_conf_data,
													self.engine_default_conf_files, self.engine_required_conf)

			self.engine_conf_proc.load()

		if self.case_args:
			self.case_conf_proc = ConfigProcessor(self.logger, self.conf_path,
												  args.case_conf_files, args.case_conf_data)

			self.case_conf_proc.load()

	@property
	def engine_conf_builder(self):
		return self.engine_conf_proc.conf_builder

	@property
	def engine_conf(self):
		return self.engine_conf_proc.conf_builder.get_conf()

	def validated_engine_conf(self, expand_vars=True):
		return self.engine_conf_proc.validated_conf(expand_vars)

	@property
	def case_conf_builder(self):
		return self.case_conf_proc.conf_builder

	@property
	def case_conf(self):
		return self.case_conf_proc.conf_builder.get_conf()

	def validated_case_conf(self, expand_vars=False):
		return self.case_conf_proc.validated_conf(expand_vars)