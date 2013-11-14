###############################################################################
#
#    Copyright 2009-2013, Universitat Pompeu Fabra
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

from wok.config.data import Data

from cmd import CommmandBuilder
from constants import ENV_PROJECT_PATH, ENV_FLOW_PATH, ENV_SCRIPT_PATH, ENV_PLATFORM_SCRIPT_PATH, CTX_EXEC
from wok.core import rtconf
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

		case_conf = case.conf.clone().expand_vars()

		# Environment variables
		env = Data.element()
		#for k, v in os.environ.items():
		#	env[k] = v
		env.merge(task.conf.get(rtconf.TASK_ENV))
		env.merge(exec_conf.get("env"))

		# Default module script path
		platform_project_path = task.conf.get(rtconf.PROJECT_PATH, case.project.path)
		flow_path = os.path.abspath(os.path.dirname(task.flow_path))
		flow_rel_path = os.path.relpath(flow_path, case.project.path)
		platform_script_path = os.path.join(platform_project_path, flow_rel_path, script_path)
		env[ENV_PROJECT_PATH] = platform_project_path
		env[ENV_FLOW_PATH] = flow_rel_path
		env[ENV_SCRIPT_PATH] = script_path
		env[ENV_PLATFORM_SCRIPT_PATH] = platform_script_path

		script = []
		
		sources = task.conf.get(rtconf.TASK_SOURCES, default=Data.list)
		if isinstance(sources, basestring):
			sources = Data.list([sources])

		for source in sources:
			script += ['source "{}"'.format(source)]
	
		if lang == "python":
			virtualenv = task.conf.get(rtconf.TASK_PYTHON_VIRTUALENV)
			if virtualenv is not None:
				#script += ["set -x"]
				#script += ["echo Activating virtualenv {} ...".format(virtualenv)]
				script += ['source "{}"'.format(os.path.join(virtualenv, "bin", "activate"))]
				#script += ["set +x"]

			#script += ["echo Running workitem ..."]

			cmd = [task.conf.get(rtconf.TASK_PYTHON_BIN, "python")]
			cmd += ["${}".format(ENV_PLATFORM_SCRIPT_PATH)]

			lib_path = task.conf.get(rtconf.TASK_PYTHON_LIBS)
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

		for key, value in self._plain_conf(Data.create(task.platform.data.context_conf(CTX_EXEC))):
			cmd += ["-D", "data.{}={}".format(key, value)]

		for key, value in self._plain_conf(task.platform.storage.context_conf(CTX_EXEC)):
			cmd += ["-D", "storage.{}={}".format(key, value)]

		script += [" ".join(cmd)]
		
		return "\n".join(script), env.to_native()
