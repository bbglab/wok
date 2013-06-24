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

from wok import logger
from wok.config.data import Data

class Storage(object):
	"""
	Abstract Storage interface.
	An Storage implementation manages any piece of information that should be shared between controller and execution nodes.
	"""

	def __init__(self, context, conf, name):
		self.context = context
		self.conf = conf
		self.name = name

		self._log = logger.get_logger(name, conf = conf.get("log"))

	@staticmethod
	def _task_config_to_element(task):
		e = Data.element(dict(
			id=task.id, name=task.name, index=task.index,
			module=task.parent.id, instance=task.instance.name,
			conf=task.conf))

		#TODO depends on module definition
		e.element("iteration", dict(strategy="dot", size=0))

		ports = e.element("ports")

		in_ports = ports.list("in")
		for i, port_node in enumerate(task.parent.in_ports):
			pe = task.in_port_data[i].fill_element(Data.element())
			pe.element("port", dict(name=port_node.name, module=port_node.parent.id))
			in_ports.append(pe)
			
		out_ports = ports.list("out")
		for i, port_node in enumerate(task.parent.out_ports):
			pe = task.out_port_data[i].fill_element(Data.element())
			pe.element("port", dict(name=port_node.name, module=port_node.parent.id))
			out_ports.append(pe)
		
		return e

class StorageContext(object):
	"An enumeration for the different storage contexts available"

	CONTROLLER = 1
	EXECUTION = 2
