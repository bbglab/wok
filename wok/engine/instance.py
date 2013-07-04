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

import sys
import os.path
import math
import re
import logging
from datetime import datetime, timedelta

from wok import logger
from wok.config import ConfigBuilder
from wok.config.data import Data
from wok.core import runstates
from wok.core.utils.sync import synchronized
from wok.core.utils.sync import Synchronizable
from wok.data import DataProvider
from wok.data.portref import PortDataRef, MergedPortDataRef
from wok.engine.nodes import *

# 2011-10-06 18:39:46,849 bfast_localalign-0000 INFO  : hello world
_LOG_RE = re.compile("^(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d,\d\d\d) (.*) (DEBUG|INFO|WARN|ERROR) : (.*)$")

class Instance(object):

	_INDENT = "  "

	def __init__(self, name, conf_builder, flow_uri, engine, platform):
		self.name = name
		self.title = name

		self.created = datetime.now()
		self.running_time = timedelta()
		
		self.conf = None
		self.conf_builder = ConfigBuilder()
		self.conf_builder.add_value("wok.__instance.name", name)
		self.conf_builder.add_builder(conf_builder)

		self.flows = []
		self.flow_uri = flow_uri
		self.root_flow = None

		self.engine = engine
		self.platform = platform

		self._state = runstates.READY

		self.root_node = None
		
		# modules by name
		self._module_index = None
	
	def initialize(self):
		self.conf = self.conf_builder()

		inst_conf = self.conf.get("wok.instance", default=Data.element)

		inst_conf["name"] = self.name
		
		self.title = inst_conf.get("title")

		self._log = logger.get_logger("wok.instance")

		self.root_flow = self.engine.projects.load_flow(self.flow_uri)

		# self._log.debug("\n" + repr(self.root_flow))

		inst_conf.element("flow", dict(
			name=self.root_flow.name,
			uri=self.flow_uri))
			#path=os.path.dirname(os.path.abspath(self.flow_uri)),
			#file=os.path.basename(self.flow_uri)))

		self._mod_conf_rules = inst_conf.get("mod_rules", default=Data.element)

		# reset state
		self._state = runstates.READY

		# create nodes tree
		self.root_node = self._create_tree(self.root_flow)

		# connect ports and create dependencies
		self._connect_ports_and_create_deps()

		# calculate priorities
		for m in self.root_node.modules:
			self._calculate_priorities(m)

		# TODO self._save_initial_state()

		#self._log.debug("Flow node tree:\n" + repr(self.root_node))

	@property
	def project(self):
		return self.root_flow.project

	@property
	def state(self):
		return self._state

	@state.setter
	def state(self, state):
		self._state = state
		self._log.debug("Instance %s updated state to %s ..." % (self.name, state))
		self.engine.notify(lock = False)

	def _save_initial_state(self):
		raise Exception("Not yet implemented")

	def _load_state(self):
		raise Exception("Not yet implemented")

	def _create_tree(self, flow_def, parent = None, namespace = ""):

		#self._log.debug("_create_tree({}, {}, {})".format(flow_def.name, parent, namespace))
		if parent is None:
			self._module_index = {}
			flow_def.depends = []

		flow = FlowNode(instance = self, parent = parent, model = flow_def, namespace = namespace)

		if len(namespace) > 0:
			ns = "{0}.{1}".format(namespace, flow_def.name)
		else: #if parent is not None:
			ns = flow_def.name
		#else:
		#	ns = ""

		# create flow port nodes
		flow.set_in_ports(self._create_port_nodes(flow, flow_def.in_ports, ns, ns))
		flow.set_out_ports(self._create_port_nodes(flow, flow_def.out_ports, ns, ns))

		# create module nodes
		for mod_def in flow_def.modules:
			if len(ns) > 0:
				mns = "{0}.{1}".format(ns, mod_def.name)
			else:
				mns = mod_def.name

			if mod_def.flow_ref is None:
				module = LeafModuleNode(instance = self, model = mod_def, parent = flow, namespace = ns)

				# create module port nodes
				module.set_in_ports(self._create_port_nodes(module, mod_def.in_ports, ns, mns))
				module.set_out_ports(self._create_port_nodes(module, mod_def.out_ports, ns, mns))
			else:
				sub_flow_def = self.engine.flow_loader.load_from_ref(mod_def.flow_ref)
				self._override_module(sub_flow_def, mod_def)
				module = self._create_tree(sub_flow_def, parent = flow, namespace = ns)
				# Import flow ports defined in the module definition
				for def_port in self._create_port_nodes(module, mod_def.in_ports, ns, mns):
					mod_port = module.get_in_port(def_port.model.name)
					if mod_port is None:
						raise Exception("The port {} is not defined in the flow {}".format(def_port.model.name, mod_def.flow_ref.uri))
					self._override_port(mod_port.model, def_port.model)
				for def_port in self._create_port_nodes(module, mod_def.out_ports, ns, mns):
					mod_port = module.get_out_port(def_port.model.name)
					if mod_port is None:
						raise Exception("The port {} is not defined in the flow {}".format(def_port.model.name, mod_def.flow_ref.uri))
			self._module_index[mns] = module

			flow.modules += [module]

		return flow

	@staticmethod
	def _create_port_nodes(module, port_defs, flow_ns, module_ns):
		port_names = set()
		ports = []
		for port_def in port_defs:
			if port_def.name in port_names:
				sb = ["Duplicated port name '{}'".format(port_def.name)]
				if len(module_ns) > 0:
					sb += [" at '{}'".format(module_ns)]
				raise Exception("".join(sb))

			port_names.add(port_def.name)

			links = []
			for link in port_def.link:
				links += ["{0}.{1}".format(flow_ns, link)]
			port_def.link = links

			port = PortNode(parent = module, model = port_def, namespace = module_ns)
			ports += [port]

		return ports

	@staticmethod
	def _override_module(ovr_mod, src_mod):
		ovr_mod.name = src_mod.name
		if src_mod.title is not None:
			ovr_mod.title = src_mod.title
		if src_mod.desc is not None:
			ovr_mod.desc = src_mod.desc
		if src_mod.enabled is not None:
			ovr_mod.enabled = src_mod.enabled
		if src_mod.serializer is not None:
			ovr_mod.serializer = mode_def.serializer
		if src_mod.wsize is not None:
			ovr_mod.wsize = mode_def.wsize
		if src_mod.conf is not None:
			if ovr_mod.conf is None:
				ovr_mod.conf = Data.element()
			ovr_mod.conf.merge(mode_def.conf)

		ovr_mod.priority = src_mod.priority
		ovr_mod.depends = src_mod.depends
		ovr_mod.flow_ref = src_mod.flow_ref

	@staticmethod
	def _override_port(ovr_port, src_port):
		if src_port.title is not None:
			ovr_port.title = src_port.title
		if src_port.desc is not None:
			ovr_port.desc = src_port.desc
		if src_port.enabled is not None:
			ovr_port.enabled = src_port.enabled
		#if src_port.serializer is not None:
		#	ovr_port.serializer = src_port.serializer
		if src_port.wsize is not None:
			ovr_port.wsize = src_port.wsize
		ovr_port.link += src_port.link

	def _flow_scope_id(self, namespace, name):
		if len(namespace) > 0:
			return "{0}.{1}".format(namespace, name)
		else:
			return name

	def _mod_scope_id(self, namespace, mname, pname):
		if len(namespace) > 0:
			return "{0}.{1}.{2}".format(namespace, mname, pname)
		else:
			return "{0}.{1}".format(mname, pname)

	def _connect_ports_and_create_deps(self):

		modules_by_id = {}
		ports_by_id = {}
		source_ports = []
		linked_ports = []
		linked_by = {}

		self._explore_ports(self.root_node, self.root_node.name,
							modules_by_id, ports_by_id, source_ports, linked_ports, linked_by)

		self._link_ports(ports_by_id, source_ports, linked_ports, linked_by)

		self._explicit_deps(modules_by_id)

		self._propagate_deps(self.root_node, set())

	def _explore_ports(self, module, namespace, modules_by_id, ports_by_id, source_ports, linked_ports, linked_by):

		# root module ports
		for port in module.in_ports + module.out_ports:
			ports_by_id[port.id] = port
			if len(port.model.link) == 0:
				source_ports += [port]
			else:
				linked_ports += [port]
				for link in port.model.link:
					if link in linked_by:
						linked_by[link].add(port.id)
					else:
						linked_by[link] = set([port.id])

		# children modules ports
		for mod in module.modules:
			modules_by_id[mod.id] = mod

			if isinstance(mod, FlowNode):
				continue

			for port in mod.in_ports + mod.out_ports:
				ports_by_id[port.id] = port
				if len(port.model.link) == 0:
					source_ports += [port]
				else:
					linked_ports += [port]
					for link in port.model.link:
						if link in linked_by:
							linked_by[link].add(port.id)
						else:
							linked_by[link] = set([port.id])

		# create port map for children modules
		for mod in module.modules:
			if len(mod.modules) > 0:
				self._explore_ports(mod, self._flow_scope_id(namespace, mod.name), modules_by_id,
									ports_by_id, source_ports, linked_ports, linked_by)

	def _link_ports(self, ports_by_id, source_ports, linked_ports, linked_by):
		visited_ports = set([port.id for port in source_ports])

		next_ports = set()

		#self._log.debug([p.id for p in source_ports])

		for port in source_ports:
			port.data = PortDataRef(port.parent.id, port.name)
			if port.id in linked_by:
				next_ports.update(set([ports_by_id[port_id] for port_id in linked_by[port.id]]))

		while len(next_ports) > 0:
			#self._log.debug([p.id for p in next_ports])
			ports = next_ports
			next_ports = set()

			for port in ports:
				if port.id in visited_ports:
					raise Exception("Circular reference: {0}".format(port.id))

				linked_data = []
				for link in port.model.link:
					if link not in ports_by_id:
						raise Exception("Port {0} references a non-existent port: {1}".format(port.id, link))

					linked_port = ports_by_id[link]
					if linked_port.data is None:
						raise Exception("Port {0} links with a non source port: {1}".format(port.id, linked_port.id))
					#elif port.parent == linked_port.parent:
					#	raise Exception("Port {} cannot be connected to another port from the same module: {}".format(port_id, linked_port.id))

					if port.serializer is not None and port.serializer != linked_port.serializer:
						raise Exception("Unmatching serializer found while linking port '{0}' [{1}] with '{2}' [{3}]".format(port.id, port.serializer, linked_port.id, linked_port.serializer))

					linked_data += [linked_port.data]

					# create implicit ports dependencies
					mod = port.parent
					dep_mod = linked_port.parent
					if not (isinstance(mod, FlowNode) and dep_mod.parent == mod) and dep_mod != mod.parent and dep_mod != mod:
						mod.depends.add(dep_mod)
						dep_mod.notify.add(mod)

				assert len(linked_data) > 0

				if len(linked_data) == 1:
					port.data = linked_data[0].link()
				else:
					port.data = MergedPortDataRef(linked_data)

				visited_ports.add(port.id)

				if port.id in linked_by:
					next_ports.update(set([ports_by_id[port_id] for port_id in linked_by[port.id]]))

		for port in linked_ports:
			if port.data is None:
				raise Exception("Unconnected port: {0}".format(port.id))

	def _explicit_deps(self, modules_by_id):
		for mod_id, module in modules_by_id.items():
			if module.model.depends is None:
				continue

			for dep_mod_name in module.model.depends:
				dep_mod_id = self._flow_scope_id(module.namespace, dep_mod_name)
				if dep_mod_id not in modules_by_id:
					raise Exception("Module {0} depends on a non existent module: {1}". format(module.id, dep_mod_id))

				dep_mod = modules_by_id[dep_mod_id]
				module.depends.add(dep_mod)
				dep_mod.notify.add(module)

	def _propagate_deps(self, module, parent_deps):
		for dep_mod in parent_deps:
			module.depends.add(dep_mod)
			dep_mod.notify.add(module)

		module.waiting = set(module.depends)

		for mod in module.modules:
			self._propagate_deps(mod, module.depends)

	def _calculate_dependencies(self, module):
		mod_req_sources = {} # module <-> sources required by the module
		source_providers = {} # source <-> modules that provide the source
		module_by_id = {} # module id <-> module
		
		self._prepare_dependency_map(module, mod_req_sources, module_by_id, source_providers)

		self._log.debug("mod_req_sources:\n" + "\n".join(sorted(["{} --> {}".format(k.id, ", ".join(["[{}]".format(id(x)) for x in v])) for k,v in mod_req_sources.items()])))
		self._log.debug("source_providers:\n" + "\n".join(sorted(["{} <-- {}".format("[{}] {}".format(id(k), k), ", ".join([x.id for x in v])) for k,v in source_providers.items()])))
		
		self._apply_dependencies(module, [], mod_req_sources, module_by_id, source_providers)
		

	def _prepare_dependency_map(self, module, mod_req_sources, module_by_id, source_providers):
		module_by_id[module.id] = module
		
		for port in module.in_ports:
			data_ref = set(port.data.refs)
			if module not in mod_req_sources:
				mod_req_sources[module] = data_ref
			else:
				mod_req_sources[module].update(data_ref)

		for port in module.out_ports:
			assert len(port.data.refs) == 1
			data_ref = port.data.refs[0]
			if data_ref not in source_providers:
				source_providers[data_ref] = set([module])
			else:
				source_providers[data_ref].add(module)

		for m in module.modules:
			self._prepare_dependency_map(m, mod_req_sources, module_by_id, source_providers)

	def _apply_dependencies(self, module, parent_deps, mod_req_sources, module_by_id, source_providers):
		parent_deps = list(parent_deps)
		module.depends = set()
		
		# explicit dependencies
		if module.model.depends is not None:
			# parent dependencies
			for dep_mod_id in parent_deps:
				dep_mod = module_by_id[dep_mod_id]
				module.depends.add(dep_mod)
				dep_mod.notify.add(module)
				
			# module dependencies
			for dep_mod_name in module.model.depends:
				if len(module.namespace) == 0:
					dep_mod_id = dep_mod_name
				else:
					dep_mod_id = "{0}.{1}".format(module.namespace, dep_mod_name)
				
				if dep_mod_id not in module_by_id:
					raise Exception("Module {0} depends on a non existent module: {1}". format(module.id, dep_mod_id))
				
				parent_deps += [dep_mod_id]
				dep_mod = module_by_id[dep_mod_id]
				module.depends.add(dep_mod)
				dep_mod.notify.add(module)
		
		# implicit dependencies
		if module in mod_req_sources:
			for source in mod_req_sources[module]:
				dep_mods = source_providers[source]
				module.depends.update(dep_mods)
				for dep_mod in dep_mods:
					dep_mod.notify.add(module)

		module.waiting = set(module.depends)

		for m in module.modules:
			self._apply_dependencies(m, parent_deps, mod_req_sources, module_by_id, source_providers)

	def _calculate_priorities(self, module, parent_priority = 0, factor = 1.0):
		if module.model.priority is not None:
			priority = module.model.priority
		else:
			priority = 0.5

		module.priority = parent_priority + (priority / factor)
		module.priority_factor = factor

		factor *= 10.0

		for m in module.modules:
			self._calculate_priorities(m, module.priority, factor)

	def apply_mod_conf_rules(self, mnode, conf):
		#self._log.debug("Module %s" % (mnode.id))
		
		for rule in self._mod_conf_rules:
			names = rule["name"]
			names_are_strings = [isinstance(a, basestring) for a in names]
			if isinstance(names, basestring):
				names = [names]
			elif not Data.is_list(names) \
					or not reduce(lambda x,y: x and y, names_are_strings):
				self._log.error("wok.instance.mod_rules[*].name must be a string or a list of strings")
				continue
			m = None
			for name in names:
				m = re.match(name, mnode.id)
				if m is not None:
					break
			if m is not None:
				#self._log.debug(">>> %s" % (str(names)))
				if "maxpar" in rule:
					mnode.maxpar = rule.get("maxpar", dtype=int)
				if "wsize" in rule:
					mnode.wsize = rule.get("wsize", dtype=int)
				if "conf" in rule:
					for entry in rule["conf"]:
						mnode.conf[entry[0]] = entry[1]

	# TODO make it an iterator
	def schedule(self):
		"""
		Schedule tasks ready for being submitted.
		:return: list of task nodes.
		"""

		if self.state != runstates.RUNNING:
			return []

		tasks = []
		require_rescheduling = True
		while require_rescheduling:
			require_rescheduling, tasks = self._schedule_module(self.root_node, tasks)
		return tasks

	def _create_module_data(self, module):
		provider = self.platform.data
		for port in module.out_ports:
			provider.remove_port_data(port)
		provider.save_module(module)

	# TODO it is not required to return the tasks list as it is already a mutable input parameter
	def _schedule_module(self, module, tasks):
		require_rescheduling = False
		if module.state == runstates.PAUSED:
			return (require_rescheduling, tasks)

		#self._log.debug("schedule(%s)" % module.id)
		
		if module.is_leaf_module:
			if module.state == runstates.READY and len(module.waiting) == 0:
				# Save module descriptor into the data provider and remove previous port data
				self._create_module_data(module)

				# Partition data stream
				module.tasks = self._partition_module(module)
				if len(module.tasks) == 0:
					self.change_module_state(module, runstates.FINISHED)
					require_rescheduling = True
					#self._log.debug("FINISHED: {}".format(repr(module)))
				else:
					for task in module.tasks:
						self.platform.data.save_task(task)

					tasks += module.tasks
					self.change_module_state(module, runstates.WAITING)
					#self._log.debug("READY: {}".format(repr(module)))
					#self._log.debug("tasks: {}".format(repr(tasks)))
			elif module.state == runstates.RUNNING:
				for task in module.tasks:
					if task.state == runstates.READY:
						tasks += [task]
				#self.change_module_state(module, runstates.RUNNING)
			#TODO elif module.state == runstates.FAILED and retrying:
		else:
			for m in module.modules:
				req_resch, tasks = self._schedule_module(m, tasks)
				require_rescheduling |= req_resch

			self.update_module_state_from_children(module, recursive=False)

		return (require_rescheduling, tasks)

	def _partition_module(self, module):
		# Calculate input sizes and the minimum wsize
		psizes = []
		mwsize = sys.maxint
		for port in module.in_ports:
			psize = 0
			for data_ref in port.data.refs:
				port_data = self.platform.data.open_port_data(self.name, data_ref)
				data_ref.size = port_data.size()
				psize += data_ref.size
			port.data.size = psize
			psizes += [psize]
			pwsize = port.wsize
			self._log.debug("[{}] {}: size={}, wsize={}".format(self.name, port.id, psize, pwsize))
			if pwsize < mwsize:
				mwsize = pwsize

		tasks = []

		if len(psizes) == 0:
			# Submit a task for the module without input ports information
			task = TaskNode(parent=module, index=0)
			tasks += [task]

			for port in module.out_ports:
				data = port.data.partition()
				task.out_port_data.append((port.name, data))
		else:
			# Check whether all inputs have the same size
			psize = psizes[0]
			for i in xrange(1, len(psizes)):
				if psizes[i] != psize:
					psize = -1
					break

			# Partition the data on input ports
			if psize == -1:
				num_partitions = 1
				self._log.warn("Unable to partition a module with inputs of different size")
			else:
				if mwsize == 0:
					num_partitions = 1
					self._log.warn("Empty port, no partitioning")
				else:
					num_partitions = int(math.ceil(psize / float(mwsize)))
					maxpar = module.maxpar
					self._log.debug("[{}] {}: maxpar={}".format(self.name, module.id, maxpar))
					if maxpar > 0 and num_partitions > maxpar:
						mwsize = int(math.ceil(psize / float(maxpar)))
						num_partitions = int(math.ceil(psize / float(mwsize)))
					self._log.debug("[{}] {}: num_par={}, psize={}, mwsize={}".format(self.name, module.id, num_partitions, psize, mwsize))

			start = 0
			for i in xrange(num_partitions):
				task = TaskNode(parent=module, index=i)
				tasks += [task]
				end = min(start + mwsize, psize)
				size = end - start

				for port in module.in_ports:
					task.in_port_data.append((port.name, port.data.slice(start, size)))
				for port in module.out_ports:
					task.out_port_data.append((port.name, port.data.partition()))

				self._log.debug("[{}] {}[{:04d}]: start={}, end={}, size={}".format(self.name, module.id, i, start, end, size))

				start += mwsize

		return tasks

	def update_state(self):
		if False and self._log.isEnabledFor(logging.DEBUG): #FIXME
			count = self.root_node.update_tasks_count_by_state()
			sb = ["[{}]".format(self.name)]
			sep = " "
			for state in runstates.STATES:
				if state.title in count:
					sb += [sep, "{}={}".format(state, count[state.title])]
					if sep == " ":
						sep = ", "
			self._log.debug("".join(sb))

	@staticmethod
	def change_module_state(module, state):
		"Changes the state of a module and updates the waiting list if finished"

		prev_state = module.state
		if prev_state == state:
			return

		module.state = state

		if state == runstates.FINISHED:
			for m in module.notify:
				if module in m.waiting:
					m.waiting.remove(module)

	def update_module_state_from_children(self, module, recursive=True):
		"""
		Updates the state of a module depending on the state of the children elements
		"""

		children_states = set()
		for m in module.children:
			children_states.add(m.state)

		prev_state = module.state
		if len(children_states) == 1:
			state = iter(children_states).next()
		else:
			if runstates.ABORTED in children_states:
				state = runstates.ABORTED
			if runstates.ABORTING in children_states:
				state = runstates.ABORTING
			elif runstates.RUNNING in children_states:
				state = runstates.RUNNING
			elif runstates.WAITING in children_states:
				state = runstates.WAITING
			elif runstates.FAILED in children_states:
				state = runstates.FAILED
			elif runstates.PAUSED in children_states:
				state = runstates.PAUSED
			elif runstates.READY in children_states:
				state = runstates.READY
			else:
				state = None

		if state is not None and state != prev_state:
#			sb = [module.id, " : ", str(prev_state)]
#			sb += [" --> ", str(state), "  {", str(", ".join(str(s) for s in children_states)), "}"]
#			if module.parent is not None:
#				sb += [" > ", module.parent.name]
#			self._log.debug("".join(sb))

			#print "%s: %s * %s --> %s" % (module.id, ", ".join(str(s) for s in sorted(children_states, key=lambda s: s.id)), str(prev_state), str(state))

			self.change_module_state(module, state)

			if module.parent is None:
				if module.state in [runstates.FINISHED, runstates.FAILED]:
					self.state = module.state
			elif recursive:
					self.update_module_state_from_children(module.parent)

	def job_ids(self, module = None, job_ids = None):
		"""
		Return all the job ids associated with the tasks that are
		not in either of the following states: finished, failed, aborted.
		Needed when it is needed to stop the active jobs.
		"""

		if module is None:
			module = self.root_node
		if job_ids is None:
			job_ids = []
		
		if module.is_leaf_module:
			for task in module.tasks:
				if task.job_id is not None and task.state not in [
						runstates.FINISHED, runstates.FAILED, runstates.ABORTED]:

					job_ids.append(task.job_id)
		else:
			for m in module.modules:
				job_ids = self.job_ids(m, job_ids)

		return job_ids

	
	def _prepare_for_continue(self, module = None):
		"""
		Resets all failed or aborted modules to allow continuation of execution.
		It is called just after a failure or stop before calling start again.
		"""

		if module is None:
			module = self.root_node

		if module.state in [runstates.FAILED, runstates.ABORTED]:
			module.state = runstates.RUNNING
			if module.is_leaf_module:
				for t in module.tasks:
					if t.state in [runstates.FAILED, runstates.ABORTED]:
						t.state = runstates.READY
			for m in module.modules:
				self._prepare_for_continue(m)

	def _reset(self, module = None):
		"Resets the state of the module and its children to an initial state."

		if module is None:
			module = self.root_node

		module.state = runstates.READY
		module.waiting = set(module.depends)
		if module.is_leaf_module:
			module.tasks = list()
#			for t in module.tasks:
#				t.state = runstates.READY
		for m in module.modules:
			self._reset(m)

	# actions -----------------------------------

	def start(self):
		if self.state not in [runstates.READY, runstates.RUNNING, runstates.PAUSED,
								runstates.FAILED, runstates.ABORTED]:
			raise Exception("Illegal action '%s' for state '%s'" % ("start", self.state))

		if self.state in [runstates.FAILED, runstates.ABORTED]:
			self._prepare_for_continue()
		
		self.state = runstates.RUNNING

		self._log.info("Instance started: {}".format(self.name))

	def pause(self):
		if self.state not in [runstates.RUNNING, runstates.PAUSED]:
			raise Exception("Illegal action '%s' for state '%s'" % ("pause", self.state))

		self.state = runstates.PAUSED

		self._log.info("Instance paused: {}".format(self.name))

	def stop(self):
		if self.state not in [runstates.RUNNING, runstates.PAUSED]:
			raise Exception("Illegal action '%s' for state '%s'" % ("stop", self.state))

		if self.state != runstates.READY:
			self.state = runstates.ABORTING

		self._log.info("Instance aborted: {}".format(self.name))

	def reset(self):
		if self.state not in [runstates.READY, runstates.PAUSED, runstates.FINISHED,
								runstates.FAILED, runstates.ABORTED]:
			raise Exception("Illegal action '%s' for state '%s'" % ("reset", self.state))

		self._reset()

		self.state = runstates.READY

		self._log.info("Instance reset: {}".format(self.name))

	def reload(self):
		self._log.info("Instance reload: {}".format(self.name))

		raise Exception("Unimplemented")

	# --------------------------------------------

	def module(self, module_id):
		"Returns a module by its id. If the module doesn't exist raises an exception."

		if module_id not in self._module_index:
			raise Exception("Module not found: %s" % module_id)

		return self._module_index[module_id]
	
	def task(self, module_id, task_index):
		"""Returns a task by module path and task index.
		It it doesn't exist raises an exception otherwise returns the task node."""

		m = self.module(module_id)
		if not isinstance(m, LeafModuleNode):
			raise Exception("Not a leaf module: %s" % module_id)

		if m.tasks is None or task_index >= len(m.tasks):
			raise Exception("Task index out of bounds: %d" % task_index)

		return m.tasks[task_index]

	#TODO
	"""
	def task_logs(self, module_id, task_index):
		if self._storage.logs.exist(self.name, module_id, task_index):
			return self._storage.logs.query(self.name, module_id, task_index)

		task = self.task(module_id, task_index)
		if task.job_id is None:
			raise Exception("Task has not been submited yet: %s" % task.id)

		job = self.engine.job_manager.job(task.job_id)
		if job is None:
			raise Exception("Task job not found: %s" % task.job_id)

		if job.output_file is None or not os.path.exists(job.output_file):
			return []

		logs = []
		for line in open(job.output_file):
			timestamp, level, name, text = parse_log(line)
			logs += [(timestamp, level, name, text)]
		
		return logs
	"""

	def modules_count_by_state(self):
		return self.root_node.update_modules_count_by_state()

	def to_element(self, e = None):
		if e is None:
			e = Data.element()

		e["name"] = self.name
		e["state"] = str(self.state)
		e["conf"] = self.conf

		self.root_node.update_tasks_count_by_state()
		self.root_node.update_modules_count_by_state()
		self.root_node.to_element(e.element("root"))

		return e

	def __repr__(self):
		sb = []
		self.repr_level(sb, 0)
		return "".join(sb)

	def repr_level(self, sb, level):
		sb += [self._INDENT * level, "Instance ", self.name, "\n"]
		level += 1
		sb += [self._INDENT * level, "State: ", str(self._state), "\n"]
		self.root_node.repr_level(sb, level)
		return level

# TODO: http://stackoverflow.com/questions/2405590/how-do-i-override-getattr-in-python-without-breaking-the-default-behavior
class SynchronizedInstance(Synchronizable):
	def __init__(self, engine, instance):
		Synchronizable.__init__(self, engine._lock)

		self.__engine = engine
		self.__instance = instance;

	def __getattr__(self, name):
		if name in ["name", "title", "state", "created", "running_time"]:
			return getattr(self.__instance, name)
		else:
			return AttributeError(name)

	@synchronized
	def module_conf(self, module_id, expanded = True):
		m = self.__instance.module(module_id)
		#print repr(m), repr(m.conf)
		if expanded:
			return m.conf.clone().expand_vars()
		else:
			return m.conf

	@synchronized
	def task_exists(self, module_path, task_index):
		try:
			self.__instance.task(module_path, task_index)
		except:
			return False
		return True

	@synchronized
	def task_logs(self, module_id, task_index):
		return self.__instance.task_logs(module_id, task_index)

	@synchronized
	def start(self):
		self.__instance.start()

	@synchronized
	def pause(self):
		self.__instance.pause()

	@synchronized
	def abort(self):
		self.__instance.abort()

	@synchronized
	def reset(self):
		self.__instance.reset()

	@synchronized
	def reload(self):
		self.__instance.reload()

	@synchronized
	def modules_count_by_state(self):
		return self.__instance.modules_count_by_state()
		
	@synchronized
	def to_element(self, e = None):
		return self.__instance.to_element(e)

	def __repr__(self):
		return repr(self.__instance)