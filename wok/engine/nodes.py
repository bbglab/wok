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

from datetime import timedelta

from wok.config.data import Data

from wok.core import runstates


class Node(object):

	_INDENT = "  "

	def __init__(self, parent, namespace=""):
		self.id = None
		self.parent = parent
		self.namespace = namespace

	@property
	def name(self):
		raise Exception("Unimplemented")

	@property
	def cname(self):
		if len(self.namespace) == 0:
			return self.name
		else:
			return ".".join([self.namespace, self.name])

	def to_element(self, e = None):
		if e is None:
			e = Data.element()

		e["ns"] = self.namespace
		e["name"] = self.name
		e["cname"] = self.cname
		
		return e

	def __str__(self):
		return self.cname

	def __repr__(self):
		sb = []
		self.repr_level(sb, 0)
		return "".join(sb)

	def repr_level(self, sb, level):
		sb.extend([self._INDENT * level,
				self.__class__.__name__, " ", self.name,
				" [", self.namespace, "]\n"])
		return level + 1

class ModelNode(Node):

	def __init__(self, parent, model, namespace=""):
		Node.__init__(self, parent, namespace)
		self.model = model

	@property
	def name(self):
		return self.model.name

class ComponentNode(ModelNode):

	def __init__(self, case, parent, model, namespace=""):
		ModelNode.__init__(self, parent, model, namespace)

		self.case = case

		self.started = self.finished = None

		self._dirty = False

		self.enabled = model.enabled

		self.state = runstates.READY
		self.substate = None

		self.priority = None
		self.priority_factor = None

		self._maxpar = model.maxpar
		self._wsize = model.wsize

		# set of modules that should finish before it can start
		self.depends = set()

		# set of modules it is waiting for
		self.waiting = set()#TODO remove

		# set of modules to notify it has finished
		self.notify = set()

		# number of workitems for each state {<state, count>}
		self.workitem_count_by_state = {}

		self.in_ports = []
		self.in_port_map = {}

		self.out_ports = []
		self.out_port_map = {}

		self.conf = None
		self._expanded_conf = None

		self.platform = None

	@property
	def dirty(self):
		return self._dirty

	@dirty.setter
	def dirty(self, dirty):
		self._dirty = dirty
		if dirty and self.parent is not None:
			self.parent.dirty = dirty

	@property
	def serializer(self):
		if self.model.serializer is not None:
			return self.model.serializer

		if self.parent is not None:
			return self.parent.serializer

		return None

	@property
	def maxpar(self):
		if self._maxpar is not None:
			return self._maxpar
		elif self.parent is not None:
			return self.parent.maxpar
		return 0

	@maxpar.setter
	def maxpar(self, value):
		self._maxpar = value

	@property
	def wsize(self):
		if self._wsize is not None:
			return self._wsize
		elif self.parent is not None:
			return self.parent.wsize
		return 1

	@wsize.setter
	def wsize(self, value):
		self._wsize = value

	@property
	def expanded_conf(self):
		if self._expanded_conf is None:
			self._expanded_conf = self.conf.clone().expand_vars()

		return self._expanded_conf

	@property
	def resources(self):
		if self.parent is None:
			res = Data.element()
		else:
			res = self.parent.resources

		if self.model.resources is not None:
			res.merge(self.model.resources)

		return res

	@property
	def elapsed(self):
		if self.started is None or self.finished is None:
			return timedelta()
		return self.finished - self.started

	def set_in_ports(self, in_ports):
		self.in_ports = in_ports
		for port in in_ports:
			self.in_port_map[port.name] = port

	def get_in_port(self, name):
		if name in self.in_port_map:
			return self.in_port_map[name]
		return None

	def set_out_ports(self, out_ports):
		self.out_ports = out_ports
		for port in out_ports:
			self.out_port_map[port.name] = port

	def get_out_port(self, name):
		if name in self.out_port_map:
			return self.out_port_map[name]
		return None

	def to_element(self, e=None):
		e = ModelNode.to_element(self, e)
		e["state"] = self.state.title
		e["substate"] = self.substate.title
		e["priority"] = self.priority
		e["depends"] = [m.cname for m in self.depends]
		e["waiting"] = [m.cname for m in self.waiting]
		e["notify"] = [m.cname for m in self.notify]
		e["enabled"] = self.enabled
		e["serializer"] = self.serializer #TODO remove
		e["maxpar"] = self.maxpar
		e["wsize"] = self.wsize
		e["started"] = self.started
		e["finished"] = self.finished
		e["conf"] = self.conf
		e["resources"] = self.resources
		e.element("tasks_count", self._tasks_count_by_state)

		ports = e.element("ports")
		in_ports = ports.list("in")
		for port in self.in_ports:
			in_ports.append(port.to_element())
		out_ports = ports.list("out")
		for port in self.out_ports:
			out_ports.append(port.to_element())		
		return e

	def repr_level(self, sb, level):
		level = ModelNode.repr_level(self, sb, level)
		sb.extend([self._INDENT * level, "Enabled: ", str(self.enabled), "\n"])
		sb.extend([self._INDENT * level, "State: ", str(self.state), "\n"])
		"""
		sb.extend([self._INDENT * level, "Attr: "])
		self.attr.repr_level(sb, level)
		sb.append("\n")
		"""
		sb.extend([self._INDENT * level, "Dirty: ", str(self.dirty), "\n"])
		if self.priority is not None:
			sb.extend([self._INDENT * level, "Priority: ", str(self.priority), "\n"])
		if self.priority_factor is not None:
			sb.extend([self._INDENT * level, "Priority factor: ", str(self.priority_factor), "\n"])
		if self.depends is not None and len(self.depends) > 0:
			sb.extend([self._INDENT * level, "Depends: ", ", ".join([m.cname for m in self.depends]), "\n"])
		if self.waiting is not None and len(self.waiting) > 0:
			sb.extend([self._INDENT * level, "Waiting: ", ", ".join([m.cname for m in self.waiting]), "\n"])
		if self.notify is not None and len(self.notify) > 0:
			sb.extend([self._INDENT * level, "Notify: ", ", ".join([m.cname for m in self.notify]), "\n"])
		if self.model.serializer is not None:
			sb.extend([self._INDENT * level, "Serializer: ", self.model.serializer, "\n"])
		if self.model.conf is not None:
			sb.extend([self._INDENT * level, "Conf: "])
			self.model.conf.repr_level(sb, level)
			sb.append("\n")
		for p in self.in_ports:
			p.repr_level(sb, level)
		for p in self.out_ports:
			p.repr_level(sb, level)
		if not self.is_leaf:
			for m in self.children:
				m.repr_level(sb, level)
		return level

class BlockNode(ComponentNode):
	def __init__(self, case, parent, model, namespace=""):
		ComponentNode.__init__(self, case, parent, model, namespace)

		# number of tasks for each state {<state, count>}
		self.task_count_by_state = {}

		self.children = []

	@property
	def is_leaf(self):
		return False

	@property
	def flow_path(self):
		return self.model.path

	def to_element(self, e = None):
		e = ComponentNode.to_element(self, e)
		components = e.list("components")
		for node in self.children:
			components.append(node.to_element())

		e.element("count_by_state", self._component_count_by_state)
		return e

class TaskNode(ComponentNode):
	def __init__(self, case, parent, model, namespace=""):
		ComponentNode.__init__(self, case, parent, model, namespace)

		#self.tasks = []
		self.workitems_count = 0

	@property
	def execution(self):
		return self.model.execution

	@property
	def is_leaf(self):
		return True

	@property
	def flow_path(self):
		return self.parent.flow_path

	def repr_level(self, sb, level):
		level = ComponentNode.repr_level(self, sb, level)
		if len(self.workitem_count_by_state) > 0:
			sb.extend([self._INDENT * level, "WorkItems:\n"])
			level += 1
			for state in sorted(self.workitem_count_by_state, key=lambda s: s.id):
				sb.extend([self._INDENT * level, str(state), ": ", str(self.workitem_count_by_state[state]), "\n"])
			level -= 1
		return level

class WorkItemNode(Node):
	def __init__(self, parent, index, id=None, namespace="", state=runstates.READY, substate=None, partition=None):
		Node.__init__(self, parent, namespace=namespace)

		self.id = id

		self.index = index

		self.state = state
		self.substate = substate

		self.partition = partition or Data.element()

		self.job_id = None
		self.job_result = None

	@property
	def name(self):
		return "{}-{:08}".format(self.parent.name, self.index)

	@property
	def priority(self):
		return max(min(self.parent.priority + (self.index / self.parent.priority_factor * 10.0), 1), 0)

	def repr_level(self, sb, level):
		level = Node.repr_level(self, sb, level)
		sb.extend([self._INDENT * level, "Index: ", str(self.index), "\n"])
		if self.substate is not None:
			sb.extend([self._INDENT * level, "Substate: ", str(self.substate), "\n"])
		sb.extend([self._INDENT * level, "State: ", str(self.state), "\n"])
		sb.extend([self._INDENT * level, "Partition: ", repr(self.partition), "\n"])

class PortNode(ModelNode):
	def __init__(self, parent, model, namespace=""):
		ModelNode.__init__(self, parent, model, namespace)

		self.data = None

	@property
	def mode(self):
		return self.model.mode

	#TODO remove
	@property
	def serializer(self):
		if self.model.serializer is not None:
			return self.model.serializer
		if self.parent is not None:
			return self.parent.serializer
		return None

	@property
	def wsize(self):
		return self.parent.wsize

	def __str__(self):
		return self.cname

	def repr_level(self, sb, level):
		level = ModelNode.repr_level(self, sb, level)
		#if self.mode == PORT_MODE_IN:
		#	mode = "In"
		#elif self.mode == PORT_MODE_OUT:
		#	mode = "Out"
		#else:
		#	mode = "Unknown"
		#sb.extend([self._INDENT * level, "Mode: ", mode, "\n"])
		sb.extend([self._INDENT * level, "Model:\n"])
		self.model.repr_level(sb, level + 1, skip_tag=True)
		#if self.model.serializer is not None:
		#	sb.extend([self._INDENT * level, "Serializer: ", self.model.serializer, "\n"])
		if self.data is not None:
			sb.extend([self._INDENT * level, "Data: ", repr(self.data), "\n"])
		return level
