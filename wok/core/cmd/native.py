###############################################################################
#
#    Copyright 2009-2011, Universitat Pompeu Fabra
#
#    This file is part of Wok.
#
#    Wok is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Wok is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses
#
###############################################################################

import os

from wok.config import COMMAND_CONF
from wok.config.data import Data

from cmd import CommmandBuilder
from wok.core.errors import MissingValueError, LanguageError


class NativeCommmandBuilder(CommmandBuilder):
	def __init__(self, conf):
		CommmandBuilder.__init__(self, conf)

	def _storage_conf(self, value, path=None):
		if path is None:
			path = []

		if not Data.is_element(value):
			yield (".".join(path), value)
		else:
			for key in value.keys():
				for k, v in self._storage_conf(value[key], path + [key]):
					yield (k, v)

	def prepare(self, task):
		execution = task.parent.execution
		exec_conf = execution.conf
		if exec_conf is None:
			exec_conf = Data.element()

		if "script_path" not in exec_conf:
			raise MissingValueError("script_path")

		script_path = exec_conf["script_path"]

		lang = exec_conf.get("language", "python")

		mod_conf = task.instance.conf.clone().expand_vars()

		cmd_conf = mod_conf.get(COMMAND_CONF, default=Data.element)
		#native_conf = cmd_conf.get("default", default=Data.element)
		#native_conf.merge(cmd_conf.get("native", default=Data.element))

		# Environment variables
		env = Data.element()
		#for k, v in os.environ.items():
		#	env[k] = v
		env.merge(cmd_conf.get("default.env"))
		env.merge(cmd_conf.get("{}.env".format(lang)))
		env.merge(exec_conf.get("env"))

		cmd = cmd_conf.get("shell.bin", "/bin/bash")
		
		args = []
		
		default_sources = cmd_conf.get("default.source", default=Data.list)
		if isinstance(default_sources, basestring):
			default_sources = Data.list([default_sources])

		sources = cmd_conf.get("{}.source".format(lang), default=Data.list)
		if isinstance(sources, basestring):
			sources = Data.list([sources])

		for source in default_sources + sources:
			if len(args) != 0:
				args += [";"]
			args += ["source", source]
	
		if lang == "python":
			if len(args) != 0:
				args += [";"]

			sources = cmd_conf.get("{}.virtualenv".format(lang), default=Data.list)
			if isinstance(sources, basestring):
				sources = Data.list([sources])

			for source in sources:
				if len(args) != 0:
					args += [";"]
				args += ["source", os.path.join(source, "bin", "activate")]

			if len(args) != 0:
				args += [";"]

			args += [cmd_conf.get("python.bin", "python")]
			args += [self._task_absolute_path(task, script_path)]

			lib_path = cmd_conf.get("python.lib_path")
			if lib_path is not None:
				if Data.is_list(lib_path):
					lib_path = ":".join(lib_path)

				if "PYTHONPATH" in env:
					env["PYTHONPATH"] = lib_path + ":" + env["PYTHONPATH"]
				else:
					env["PYTHONPATH"] = lib_path
		else:
			raise LanguageError(lang)

		args += ["-D", "instance_name={}".format(task.instance.name),
				"-D", "module_path={}".format(".".join([task.parent.namespace, task.parent.name])),
				"-D", "task_index={}".format(task.index)]

		for key, value in self._storage_conf(task.instance.engine.storage.basic_conf):
			args += ["-D", "storage.{}={}".format(key, value)]

		args = ["-c", " ".join(args)]
		
		return cmd, args, env.to_native()

	@staticmethod
	def _task_absolute_path(task, path):
		if os.path.isabs(path):
			return path

		flow_path = os.path.dirname(task.parent.flow_path)
		return os.path.abspath(os.path.join(flow_path, path))
