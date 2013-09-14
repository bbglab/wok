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
from constants import FLOW_PATH, SCRIPT_PATH, MODULE_SCRIPT_PATH
from wok.core.errors import MissingValueError, LanguageError


class NativeCommmandBuilder(CommmandBuilder):
	def _plain_conf(self, value, path=None):
		if path is None:
			path = []

		if not Data.is_element(value):
			yield (".".join(path), value)
		else:
			for key in value.keys():
				for k, v in self._plain_conf(value[key], path + [key]):
					yield (k, v)

	def prepare(self, case, task, index):
		execution = task.execution
		exec_conf = execution.conf
		if exec_conf is None:
			exec_conf = Data.element()

		if "script_path" not in exec_conf:
			raise MissingValueError("script_path")

		script_path = exec_conf["script_path"]

		lang = exec_conf.get("language", "python")

		mod_conf = case.conf.clone().expand_vars()

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

		# Default module script path
		flow_path = os.path.abspath(os.path.dirname(task.flow_path))
		env[FLOW_PATH] = flow_path
		env[SCRIPT_PATH] = script_path
		env[MODULE_SCRIPT_PATH] = os.path.join(flow_path, script_path)

		script = []
		
		default_sources = cmd_conf.get("default.source", default=Data.list)
		if isinstance(default_sources, basestring):
			default_sources = Data.list([default_sources])

		sources = cmd_conf.get("{}.source".format(lang), default=Data.list)
		if isinstance(sources, basestring):
			sources = Data.list([sources])

		for source in default_sources + sources:
			script += ["source {}".format(source)]
	
		if lang == "python":
			virtualenvs = cmd_conf.get("{}.virtualenv".format(lang), default=Data.list)
			if isinstance(virtualenvs, basestring):
				virtualenvs = Data.list([virtualenvs])

			#script += ["set -x"]
			for virtualenv in virtualenvs:
				#script += ["echo Activating virtualenv {} ...".format(virtualenv)]
				script += ["source '{}'".format(os.path.join(virtualenv, "bin", "activate"))]
			#script += ["set +x"]

			#script += ["echo Running workitem ..."]

			cmd = [cmd_conf.get("python.bin", "python")]
			cmd += [script_path if os.path.isabs(script_path) else "${}".format(MODULE_SCRIPT_PATH)]

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

		cmd += ["-D", "case={}".format(case.name),
				"-D", "task={}".format(task.cname),
				"-D", "index={}".format(index)]

		#for key, value in self._storage_conf(workitem.case.engine.storage.basic_conf):
		#	cmd += ["-D", "storage.{}={}".format(key, value)]

		for key, value in self._plain_conf(Data.create(case.platform.data.bootstrap_conf)):
			cmd += ["-D", "data.{}={}".format(key, value)]

		script += [" ".join(cmd)]
		
		return "\n".join(script), env.to_native()
