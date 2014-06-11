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

import sys
import math
import re
from datetime import datetime
from collections import deque

from sqlalchemy import func, distinct

from wok import logger
from wok.config.data import Data
from wok.core import runstates, rtconf
from wok.core.utils.sync import synchronized
from wok.core.utils.sync import Synchronizable
from wok.core.utils.proxies import ReadOnlyProxy
from wok.data.portref import PortDataRef, MergedPortDataRef

from nodes import *
import db

# 2011-10-06 18:39:46,849 bfast_localalign-0000 INFO  : hello world
_LOG_RE = re.compile("^(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d,\d\d\d) (.*) (DEBUG|INFO|WARN|ERROR) : (.*)$")

class Case(object):

	_INDENT = "  "

	def __init__(self, name, conf_builder, project, flow, container_name, engine):
		self.name = name
		self.conf_builder = conf_builder
		self.project = project
		self.root_flow = flow
		self.container_name = container_name

		self.engine = engine

		self.title = flow.title
		self.created = datetime.now()
		self.flow_uri = "{}:{}".format(project.name, flow.name) # FIXME flow.uri

		self._log = logger.get_logger("wok.case")

		self._initialize_conf()

		# reset state
		self._state = runstates.PAUSED

		# components by name
		self._component_by_cname = None

		# create nodes tree
		self.root_node = self._create_tree(self.root_flow)

		# calculate components configuration
		self._apply_component_conf()

		# connect ports and create dependencies
		self._connect_ports_and_create_deps()

		# calculate the list of tasks in dependency order
		self.tasks = self._tasks_in_dependency_order()

		# list of used platforms
		self.platforms = self._used_platforms()

		# calculate priorities
		for node in self.root_node.children:
			self._calculate_priorities(node)

		# Track the number of active workitems
		self.num_active_workitems = 0

		# Whether this case is marked for removal when all the jobs finish
		self.removed = False

		#self._log.debug("Flow node tree:\n" + repr(self.root_node))
	
	def _initialize_conf(self):
		# project defined config
		self.conf = conf = self.project.get_conf()

		# user defined config
		self.user_conf = self.conf_builder.get_conf()
		if "wok" in self.user_conf:
			self.user_conf.delete("wok.work_path", "wok.projects", "wok.platforms", "wok.logging")
		conf.merge(self.user_conf)

		# runtime config
		conf[rtconf.CASE_NAME] = self.name
		conf[rtconf.FLOW] = Data.element(dict(
			name=self.root_flow.name,
			uri=self.flow_uri))
			#path=os.path.dirname(os.path.abspath(self.flow_uri)),
			#file=os.path.basename(self.flow_uri)))

	def persist(self, session):
		case = db.Case(
			name=self.name,
			title=self.title,
			created=self.created,
			project=self.project.name,
			flow=self.root_flow.name,
			storage=self.container_name,
			conf=self.conf,
			state=self._state)

		session.add(case)

		nodes = deque([(None, self.root_node)])
		while len(nodes) > 0:
			parent, node = nodes.popleft()
			if node.is_leaf:
				c = self.__persist_component(db.Task, case, parent, node,
										type=None, #TODO
										execution=None) #TODO
			else:
				c = self.__persist_component(db.Block, case, parent, node,
										library=node.model.library,
										version=node.model.version,
										flow_path=node.model.path)

				nodes.extend([(c, n) for n in node.children])

			session.add(c)

		session.commit()

		self.id = case.id

		for c in session.query(db.Component).filter(db.Component.case_id == case.id):
			node = self.component(c.cname)
			node.id = c.id

	def __persist_component(self, cls, case, parent, node, **kwargs):
		c = cls(
				case=case,
				parent=parent,
				ns=node.namespace,
				name=node.name,
				cname=node.cname,
				title=node.model.title,
				desc=node.model.desc,
				enabled=node.model.enabled,
				maxpar=node.model.maxpar,
				wsize=node.model.wsize,
				priority=node.priority,
				priority_factor=node.priority_factor,
				resources=node.resources,
				state=node.state,
				conf=node.conf,
				**kwargs)

		#TODO params
		#c.params = []

		#TODO ports
		#c.ports = []

		return c

	def remove(self, session):
		self._log.debug("Removing case state from the database ...")
		cmps = session.query(db.Component.id).filter(db.Component.case_id == self.id).subquery()
		self._log.debug("  -> tasks ...")
		session.query(db.Task).filter(db.Task.id.in_(cmps)).delete(synchronize_session='fetch')
		self._log.debug("  -> blocks ...")
		session.query(db.Block).filter(db.Block.id.in_(cmps)).delete(synchronize_session='fetch')
		self._log.debug("  -> components ...")
		session.query(db.Component).filter(db.Component.case_id == self.id).delete()
		self._log.debug("  -> workitems ...")
		session.query(db.WorkItem).filter(db.WorkItem.case_id == self.id).delete()
		self._log.debug("  -> case ...")
		session.query(db.Case).filter(db.Case.id == self.id).delete()

	@property
	def state(self):
		return self._state

	@state.setter
	def state(self, state):
		self._state = state
		self._log.debug("Case {} updated state to {} ...".format(self.name, state))
		self.engine.notify(lock=False)

	def _create_tree(self, flow_def, parent=None, namespace=""):

		#self._log.debug("_create_tree({}, {}, {})".format(flow_def.name, parent, namespace))
		if parent is None:
			self._component_by_cname = {}
			flow_def.depends = []

		flow = BlockNode(case=self, parent=parent, model=flow_def, namespace=namespace)

		if len(namespace) > 0:
			ns = "{}.{}".format(namespace, flow_def.name)
		else: #if parent is not None:
			ns = flow_def.name
		#else:
		#	ns = ""

		self._component_by_cname[ns] = flow

		# create flow port nodes
		flow.set_in_ports(self._create_port_nodes(flow, flow_def.in_ports, ns, ns))
		flow.set_out_ports(self._create_port_nodes(flow, flow_def.out_ports, ns, ns))

		# create module nodes
		for comp_def in flow_def.modules:
			if len(ns) > 0:
				mns = "{}.{}".format(ns, comp_def.name)
			else:
				mns = comp_def.name

			if comp_def.flow_ref is None:
				comp = TaskNode(case=self, model=comp_def, parent=flow, namespace=ns)

				self._component_by_cname[mns] = comp

				# create module port nodes
				comp.set_in_ports(self._create_port_nodes(comp, comp_def.in_ports, ns, mns))
				comp.set_out_ports(self._create_port_nodes(comp, comp_def.out_ports, ns, mns))
			else:
				sub_flow_def = self.project.load_from_ref(comp_def.flow_ref)
				self._override_component(sub_flow_def, comp_def)
				comp = self._create_tree(sub_flow_def, parent=flow, namespace=ns)
				# Import flow ports defined in the module definition
				for def_port in self._create_port_nodes(comp, comp_def.in_ports, ns, mns):
					mod_port = comp.get_in_port(def_port.model.name)
					if mod_port is None:
						raise Exception("The port {} is not defined in the flow {}".format(def_port.model.name, comp_def.flow_ref.uri))
					self._override_port(mod_port.model, def_port.model)
				for def_port in self._create_port_nodes(comp, comp_def.out_ports, ns, mns):
					mod_port = comp.get_out_port(def_port.model.name)
					if mod_port is None:
						raise Exception("The port {} is not defined in the flow {}".format(def_port.model.name, comp_def.flow_ref.uri))

			flow.children += [comp]

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
				links += ["{}.{}".format(flow_ns, link)]
			port_def.link = links

			port = PortNode(parent = module, model = port_def, namespace = module_ns)
			ports += [port]

		return ports

	@staticmethod
	def _override_component(ovr, src):
		ovr.name = src.name
		if src.title is not None:
			ovr.title = src.title
		if src.desc is not None:
			ovr.desc = src.desc
		if src.enabled is not None:
			ovr.enabled = src.enabled
		if src.serializer is not None:
			ovr.serializer = mode_def.serializer
		if src.wsize is not None:
			ovr.wsize = mode_def.wsize
		if src.conf is not None:
			if ovr.conf is None:
				ovr.conf = Data.element()
			ovr.conf.merge(mode_def.conf)

		ovr.priority = src.priority
		ovr.depends = src.depends
		ovr.flow_ref = src.flow_ref

	@staticmethod
	def _override_port(ovr, src):
		if src.title is not None:
			ovr.title = src.title
		if src.desc is not None:
			ovr.desc = src.desc
		if src.enabled is not None:
			ovr.enabled = src.enabled
		#if src.serializer is not None:
		#	ovr.serializer = src.serializer
		if src.wsize is not None:
			ovr.wsize = src.wsize
		ovr.link += src.link

	def _flow_scope_cname(self, namespace, name):
		if len(namespace) > 0:
			return "{}.{}".format(namespace, name)
		else:
			return name

	def _component_scope_id(self, namespace, component_name, port_name):
		if len(namespace) > 0:
			return "{}.{}.{}".format(namespace, component_name, port_name)
		else:
			return "{}.{}".format(component_name, port_name)

	def _connect_ports_and_create_deps(self):

		components_by_id = {}
		ports_by_id = {}
		source_ports = []
		linked_ports = []
		linked_by = {}

		self._explore_ports(self.root_node, self.root_node.name,
							components_by_id, ports_by_id, source_ports, linked_ports, linked_by)

		self._link_ports(ports_by_id, source_ports, linked_ports, linked_by)

		self._explicit_deps(components_by_id)

		self._propagate_deps(self.root_node, set())

	def _explore_ports(self, component, namespace,
					   components_by_id, ports_by_id, source_ports, linked_ports, linked_by):

		# root component ports
		for port in component.in_ports + component.out_ports:
			ports_by_id[port.cname] = port
			if len(port.model.link) == 0:
				source_ports += [port]
			else:
				linked_ports += [port]
				for link in port.model.link:
					if link in linked_by:
						linked_by[link].add(port.cname)
					else:
						linked_by[link] = set([port.cname])

		# children modules ports
		for n in component.children:
			components_by_id[n.cname] = n

			if isinstance(n, BlockNode):
				continue

			for port in n.in_ports + n.out_ports:
				ports_by_id[port.cname] = port
				if len(port.model.link) == 0:
					source_ports += [port]
				else:
					linked_ports += [port]
					for link in port.model.link:
						if link in linked_by:
							linked_by[link].add(port.cname)
						else:
							linked_by[link] = set([port.cname])

		# create port map for children modules
		for n in component.children:
			if not n.is_leaf:
				self._explore_ports(n, self._flow_scope_cname(namespace, n.name), components_by_id,
									ports_by_id, source_ports, linked_ports, linked_by)

	def _link_ports(self, ports_by_id, source_ports, linked_ports, linked_by):
		visited_ports = set([port.cname for port in source_ports])

		next_ports = set()

		#self._log.debug([p.cname for p in source_ports])

		for port in source_ports:
			port.data = PortDataRef(port.parent.cname, port.name)
			if port.cname in linked_by:
				next_ports.update(set([ports_by_id[port_id] for port_id in linked_by[port.cname]]))

		while len(next_ports) > 0:
			#self._log.debug([p.cname for p in next_ports])
			ports = next_ports
			next_ports = set()

			for port in ports:
				if port.cname in visited_ports:
					raise Exception("Circular reference: {}".format(port.cname))

				linked_data = []
				for link in port.model.link:
					if link not in ports_by_id:
						raise Exception("Port {} references a non-existent port: {}".format(port.cname, link))

					linked_port = ports_by_id[link]
					if linked_port.data is None:
						raise Exception("Port {} links with a non source port: {}".format(port.cname, linked_port.cname))
					#elif port.parent == linked_port.parent:
					#	raise Exception("Port {} cannot be connected to another port from the same module: {}".format(port_id, linked_port.cname))

					if port.serializer is not None and port.serializer != linked_port.serializer:
						raise Exception("Unmatching serializer found while linking port '{}' [{}] with '{}' [{}]".format(port.cname, port.serializer, linked_port.cname, linked_port.serializer))

					linked_data += [linked_port.data]

					# create implicit ports dependencies
					mod = port.parent
					dep_mod = linked_port.parent
					if not (isinstance(mod, BlockNode) and dep_mod.parent == mod) and dep_mod != mod.parent and dep_mod != mod:
						mod.depends.add(dep_mod)
						dep_mod.notify.add(mod)

				assert len(linked_data) > 0

				if len(linked_data) == 1:
					port.data = linked_data[0].link()
				else:
					port.data = MergedPortDataRef(linked_data)

				visited_ports.add(port.cname)

				if port.cname in linked_by:
					next_ports.update(set([ports_by_id[port_id] for port_id in linked_by[port.cname]]))

		for port in linked_ports:
			if port.data is None:
				raise Exception("Unconnected port: {}".format(port.cname))

	def _explicit_deps(self, modules_by_id):
		for mod_id, module in modules_by_id.items():
			if module.model.depends is None:
				continue

			for dep_mod_name in module.model.depends:
				dep_mod_id = self._flow_scope_cname(module.namespace, dep_mod_name)
				if dep_mod_id not in modules_by_id:
					raise Exception("Module {} depends on a non existent module: {}". format(module.cname, dep_mod_id))

				dep_mod = modules_by_id[dep_mod_id]
				module.depends.add(dep_mod)
				dep_mod.notify.add(module)

	def _propagate_deps(self, component, parent_deps):
		for dep_component in parent_deps:
			component.depends.add(dep_component)
			dep_component.notify.add(component)

		component.waiting = set(component.depends)

		if not component.is_leaf:
			for child in component.children:
				self._propagate_deps(child, component.depends)

	def _calculate_dependencies(self, component):
		comp_req_sources = {} # component <-> sources required by the component
		source_providers = {} # source <-> modules that provide the source
		component_by_cname = {} # component cname <-> component
		
		self._prepare_dependency_map(component, comp_req_sources, component_by_cname, source_providers)

		#self._log.debug("mod_req_sources:\n" + "\n".join(sorted(["{} --> {}".format(k.cname, ", ".join(["[{}]".format(id(x)) for x in v])) for k,v in comp_req_sources.items()])))
		#self._log.debug("source_providers:\n" + "\n".join(sorted(["{} <-- {}".format("[{}] {}".format(id(k), k), ", ".join([x.cname for x in v])) for k,v in source_providers.items()])))
		
		self._apply_dependencies(component, [], comp_req_sources, component_by_cname, source_providers)
		

	def _prepare_dependency_map(self, component, comp_req_sources, component_by_cname, source_providers):
		component_by_cname[component.cname] = component
		
		for port in component.in_ports:
			data_ref = set(port.data.refs)
			if component not in comp_req_sources:
				comp_req_sources[component] = data_ref
			else:
				comp_req_sources[component].update(data_ref)

		for port in component.out_ports:
			assert len(port.data.refs) == 1
			data_ref = port.data.refs[0]
			if data_ref not in source_providers:
				source_providers[data_ref] = set([component])
			else:
				source_providers[data_ref].add(component)

		if not component.is_leaf:
			for child in component.children:
				self._prepare_dependency_map(child, comp_req_sources, component_by_cname, source_providers)

	def _apply_dependencies(self, component, parent_deps, comp_req_sources, component_by_cname, source_providers):
		parent_deps = list(parent_deps)
		component.depends = set()
		
		# explicit dependencies
		if component.model.depends is not None:
			# parent dependencies
			for dep_cname in parent_deps:
				dep_comp = component_by_cname[dep_cname]
				component.depends.add(dep_comp)
				dep_comp.notify.add(component)
				
			# component dependencies
			for dep_name in component.model.depends:
				if len(component.namespace) == 0:
					dep_cname = dep_name
				else:
					dep_cname = "{}.{}".format(component.namespace, dep_name)
				
				if dep_cname not in component_by_cname:
					raise Exception("Component {} depends on a non existent component: {}". format(component.cname, dep_cname))
				
				parent_deps += [dep_cname]
				dep_comp = component_by_cname[dep_cname]
				component.depends.add(dep_comp)
				dep_comp.notify.add(component)
		
		# implicit dependencies
		if component in comp_req_sources:
			for source in comp_req_sources[component]:
				dep_mods = source_providers[source]
				component.depends.update(dep_mods)
				for dep_comp in dep_mods:
					dep_comp.notify.add(component)

		component.waiting = set(component.depends)

		if not component.is_leaf:
			for child in component.children:
				self._apply_dependencies(child, parent_deps, comp_req_sources, component_by_cname, source_providers)

	def _tasks_in_dependency_order(self):
		tasks = []

		i = 0
		dep_count = {} # {task, count} how many tasks depend on the task
		q = deque(self.root_node.children)
		while len(q) > 0:
			comp = q.popleft()
			dep_count[comp] = [i, len(comp.depends)]
			i += 1
			if not comp.is_leaf:
				q.extend(comp.children)

		comps = [[order, comp] for comp, (order, count) in dep_count.items() if count == 0]
		while len(comps) > 0:
			t = []
			for order, comp in comps:
				del dep_count[comp]
				for dep in comp.notify:
					dep_count[dep][1] -= 1
				if comp.is_leaf:
					t += [(order, comp)]
			tasks += [comp for order, comp in sorted(t, key=lambda c: c[0])]
			comps = [[order, comp] for comp, (order, count) in dep_count.items() if count == 0]

		if len(dep_count) > 0:
			self._log.warn("Some components got excluded from the list of tasks ordered by its dependencies:\n  {}".format(
				"\n  ".join(sorted([comp.cname for comp in dep_count.keys()]))))

		return tasks

	def _tasks_under_block(self, component):
		tasks = []
		q = deque(component.children)
		while len(q) > 0:
			comp = q.popleft()
			if comp.is_leaf:
				tasks += [comp]
			else:
				q.extend(comp.children)
		return tasks

	def _used_platforms(self):
		default_platform_target = self.engine.default_platform.name
		platforms = []
		names = set()
		for task in self.tasks:
			platform_target = task.conf.get(rtconf.PLATFORM_TARGET, default_platform_target)
			platform = self.engine.platform(platform_target)
			if platform is None:
				raise Exception("Task '{}' references a platform that does not exists: {}".format(task.cname, platform_target))
			if platform.name not in names:
				platforms += [platform]
				names.add(platform.name)
		return platforms

	def _calculate_priorities(self, component, parent_priority=0, factor=1.0):
		if component.model.priority is not None:
			priority = component.model.priority
		else:
			priority = 0.5

		component.priority = parent_priority + (priority / factor)
		component.priority_factor = factor

		factor *= 10.0

		if not component.is_leaf:
			for child in component.children:
				self._calculate_priorities(child, component.priority, factor)

	def _materialize_component_conf(self, component, default_platform_target):
		conf = component.conf

		component.enabled = conf.get(rtconf.TASK_ENABLED, component.enabled)
		component.maxpar = conf.get(rtconf.TASK_MAXPAR, component.maxpar)
		component.wsize = conf.get(rtconf.TASK_WSIZE, component.wsize)

		platform_target = conf.get(rtconf.PLATFORM_TARGET, default_platform_target)
		if platform_target is not None:
			component.platform = self.engine.platform(platform_target)
		else:
			component.platform = self.engine.default_platform

	def _apply_component_conf(self, component=None):
		if component is None:
			component = self.root_node

		default_platform_target = self.engine.default_platform.name

		# apply project configuration
		conf = self.project.get_conf(task_name=component.cname, platform_name=default_platform_target)

		# apply configuration defined in the workflow model
		if component.model.conf is not None:
			conf.merge(component.model.conf)

		# apply user configuration

		#print component.cname, ">>>", conf, ">>>>>>>>>>>>>>>>", self.user_conf

		conf.merge(self.user_conf)

		if rtconf.PROJECT_PATH not in conf:
			conf[rtconf.PROJECT_PATH] = self.project.path

		component.conf = conf.expand_vars()

		self._materialize_component_conf(component, default_platform_target)

		if not component.is_leaf:
			for child in component.children:
				self._apply_component_conf(child)

	def _create_task_data(self, task):
		"""
		Prepare the data provider for a new scheduled task.
		"""

		provider = task.platform.data
		for port in task.out_ports:
			provider.remove_port_data(port)
		provider.save_task(task)

	def schedule(self, session):
		"""
		Schedule and partition tasks.
		Generated work items are persisted into the database.
		:return: list of updated modules.
		"""

		if self._state in [runstates.PAUSED] + runstates.TERMINAL_STATES:
			return []

		updated_components = []
		require_rescheduling = True
		while require_rescheduling:
			require_rescheduling = self._schedule_component(session, self.root_node, updated_components)
		return updated_components

	def _schedule_component(self, session, component, updated_components):
		"""
		Schedule and partition the component recursively.
		Generated work items are persisted into the database.
		:return: whether some component changed state that requires the full tree to be scheduled again
		"""

		require_rescheduling = False
		if component.state == runstates.PAUSED:
			return require_rescheduling
		
		if component.is_leaf:
			if component.state == runstates.READY and len(component.waiting) == 0:

				task_id, = session.query(db.Task.id).filter(db.Task.case_id == self.id)\
													.filter(db.Task.cname == component.cname).one()

				# Save module descriptor into the data provider and remove previous port data
				self._create_task_data(component)

				# Partition data stream
				count = 0
				for workitem in self._partition_task(component):
					component.platform.data.save_workitem(workitem)

					session.add(db.WorkItem(
						case_id=self.id,
						task_id=task_id,
						ns=workitem.namespace,
						name=workitem.name,
						cname=workitem.cname,
						index=workitem.index,
						state=workitem.state,
						substate=workitem.substate,
						priority=workitem.priority,
						platform=component.platform.name))

					session.commit()

					count += 1

				self.num_active_workitems += count

				if count == 0:
					self.change_component_state(component, runstates.FINISHED)
					require_rescheduling = True
				else:
					self.change_component_state(component, runstates.WAITING)

				session.commit()

				updated_components += [component]

			#TODO elif component.state == runstates.FAILED and retrying:
		else:
			for node in component.children:
				require_rescheduling |= self._schedule_component(session, node, updated_components)

			#if self.update_component_state_from_children(component, recursive=False):
			#	updated_components += [component]

		return require_rescheduling

	def _partition_task(self, task):
		"""
		Partition the input data for a task into work items. It is an iterator of WorkItems.
		"""

		# Calculate input sizes and the minimum wsize
		psizes = []
		mwsize = sys.maxint
		for port in task.in_ports:
			psize = 0
			for data_ref in port.data.refs:
				port_data = task.platform.data.open_port_data(self.name, data_ref)
				data_ref.size = port_data.size()
				psize += data_ref.size
			port.data.size = psize
			psizes += [psize]
			pwsize = port.wsize
			self._log.debug("[{}] {}: size={}, wsize={}".format(self.name, port.cname, psize, pwsize))
			if pwsize < mwsize:
				mwsize = pwsize

		if len(psizes) == 0:
			# Submit a task for the module without input ports information
			workitem = WorkItemNode(parent=task, index=0, namespace=task.namespace)

			out_ports = []
			for port in task.out_ports:
				port_data = port.data.partition()
				out_ports += [dict(
					name=port.name,
					data=port_data.to_native())]

			workitem.partition["ports"] = Data.create({"out" : out_ports})

			yield workitem
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
				self._log.warn("[{}] Unable to partition a task with input ports of different size".format(task.cname))
			else:
				if mwsize == 0:
					num_partitions = 1
					self._log.warn("[{}] Empty port, no partitioning".format(task.cname))
				else:
					num_partitions = int(math.ceil(psize / float(mwsize)))
					maxpar = task.maxpar
					self._log.debug("[{}] {}: maxpar={}".format(self.name, task.cname, maxpar))
					if maxpar > 0 and num_partitions > maxpar:
						mwsize = int(math.ceil(psize / float(maxpar)))
						num_partitions = int(math.ceil(psize / float(mwsize)))
					self._log.debug("[{}] {}: num_par={}, psize={}, mwsize={}".format(
										self.name, task.cname, num_partitions, psize, mwsize))

			start = 0
			for i in xrange(num_partitions):
				workitem = WorkItemNode(parent=task, index=i, namespace=task.namespace)

				end = min(start + mwsize, psize)
				size = end - start

				in_ports = []
				for port in task.in_ports:
					#workitem.in_port_data.append((port.name, port.data.slice(start, size)))
					port_data = port.data.slice(start, size)
					in_ports += [dict(
						name=port.name,
						data=port_data.to_native())]

				out_ports = []
				for port in task.out_ports:
					#workitem.out_port_data.append((port.name, port.data.partition()))
					port_data = port.data.partition()
					out_ports += [dict(
						name=port.name,
						data=port_data.to_native())]

				workitem.partition["ports"] = Data.create({"in" : in_ports, "out" : out_ports})

				self._log.debug("[{}] {}[{:04d}]: start={}, end={}, size={}".format(self.name, task.cname, i, start, end, size))

				start += mwsize

				yield workitem

	def change_component_state(self, component, state):
		"Changes the state of a component and updates the waiting list if finished"

		prev_state = component.state
		if prev_state == state:
			return

		component.dirty = True
		component.state = state

		if state == runstates.FINISHED:
			for notify_component in component.notify:
				if component in notify_component.waiting:
					notify_component.waiting.remove(component)

		if state == runstates.RUNNING:
			if component.started is None:
				component.started = datetime.now()
				component.finished = None

			#self.engine.case_started.send(self)

		if state in runstates.TERMINAL_STATES:
			component.finished = datetime.now()
			if component.started is None:
				component.started = component.finished

			#self.engine.case_finished.send(self)

	def update_states(self, session, component=None):
		if component is None:
			component = self.root_node

		if component.is_leaf:
			children_states = set([state for state, in session.query(distinct(db.WorkItem.state)).filter(
																		db.WorkItem.task_id == component.id).all()])
		else:
			children_states = set()
			for child in component.children:
				self.update_states(session, child)
				children_states.add(child.state)

		if self.update_component_state(component, children_states):
			session.query(db.Component).filter(db.Component.id == component.id)\
					.update({db.Component.state : component.state,
							 db.Component.started : component.started,
							 db.Component.finished : component.finished})
			if component == self.root_node and self._state != runstates.PAUSED:
				prev_state = self.state
				if component.state != prev_state:
					self.state = component.state # update case state from the root node
					session.query(db.Case).filter(db.Case.id == self.id).update({db.Case.state : self.state})

	def update_component_state(self, component, children_states):
		"""
		Updates the state of a component depending on the state of the children elements
		:param component: component to update
		:param children_states: set of children states
		:return: True if component state changed, False otherwise
		"""

		state = None
		prev_state = component.state
		cnt = len(children_states)
		if cnt == 1:
			state = iter(children_states).next()
		elif cnt > 1:
			winner_states = [
				runstates.ABORTING,
				runstates.ABORTED,
				runstates.FAILED,
				runstates.RUNNING,
				runstates.WAITING,
				runstates.PAUSED,
				runstates.READY]

			for winner_state in winner_states:
				if winner_state in children_states:
					state = winner_state
					break

		if state is not None and state != prev_state:
			self.change_component_state(component, state)
			return True

		return False

	def update_count_by_state(self, session, component=None):
		if component is None:
			component = self.root_node

		task_count_by_state = {}
		workitem_count_by_state = {}

		if component.is_leaf:
			if component.dirty:
				query = session.query(db.WorkItem.state, func.count(db.WorkItem.state))\
								.filter(db.WorkItem.task_id == component.id)\
								.group_by(db.WorkItem.state)

				for state, count in query:
					workitem_count_by_state[state] = count
			else:
				workitem_count_by_state = component.workitem_count_by_state
		else:
			# count children only if there is some dirty child
			#if component.dirty:
			for child in component.children:
				child_task_count, child_workitem_count = self.update_count_by_state(session, child)

				if child.is_leaf:
					state = child.state
					if state not in task_count_by_state:
						task_count_by_state[state] = 1
					else:
						task_count_by_state[state] += 1
				else:
					for state, count in child_task_count.items():
						if state not in task_count_by_state:
							task_count_by_state[state] = count
						else:
							task_count_by_state[state] += count

				for state, count in child_workitem_count.items():
					if state not in workitem_count_by_state:
						workitem_count_by_state[state] = count
					else:
						workitem_count_by_state[state] += count
			#else:
			#	task_count_by_state = component.task_count_by_state
			#	workitem_count_by_state = component.workitem_count_by_state

		component.task_count_by_state = task_count_by_state
		component.workitem_count_by_state = workitem_count_by_state
		return task_count_by_state, workitem_count_by_state

	def clean_components(self, session, component=None):
		if component is None:
			component = self.root_node

		component.dirty = False

		if not component.is_leaf:
			for child in component.children:
				self.clean_components(session, child)
	
	def _reset_failures(self, session):
		"""
		Resets all failed or aborted tasks to allow continuation of execution.
		It is called before calling start when there has been a failure or stop.
		"""

		session.query(db.WorkItem).filter(db.WorkItem.case_id == self.id)\
			.filter(db.WorkItem.state._in([runstates.FAILED, runstates.ABORTED]))\
			.update({db.WorkItem.state : runstates.READY, db.WorkItem.substate : None})

		self.update_states(session)

	#FIXME review
	def _reset(self, module = None):
		"Resets the state of the module and its children to an initial state."

		if module is None:
			module = self.root_node

		module.state = runstates.READY
		module.waiting = set(module.depends)
		if module.is_leaf:
			module.tasks = list()
#			for t in module.tasks:
#				t.state = runstates.READY
		for m in module.children:
			self._reset(m)

	# actions -----------------------------------

	def start(self):
		if self.state not in [runstates.READY, runstates.RUNNING, runstates.PAUSED,
								runstates.FAILED, runstates.ABORTED]:
			raise Exception("Illegal action '{}' for state '{}'".format("start", self.state))

		session = db.Session()

		if self.state in [runstates.FAILED, runstates.ABORTED]:
			self._reset_failures(session)
		
		self.state = runstates.READY
		session.query(db.Case).filter(db.Case.id == self.id).update({db.Case.state : self.state})

		session.commit()
		session.close()

		self._log.info("[{}] Case started".format(self.name))

	def pause(self):
		if self.state not in [runstates.RUNNING, runstates.PAUSED]:
			raise Exception("Illegal action '{}' for state '{}'".format("pause", self.state))

		# FIXME update component state in the database
		self.state = runstates.PAUSED

		self._log.info("[{}] Case paused".format(self.name))

	def stop(self):
		if self.state not in [runstates.RUNNING, runstates.PAUSED]:
			raise Exception("Illegal action '{}' for state '{}'".format("stop", self.state))

		# FIXME update component state in the database
		if self.state != runstates.READY:
			self.state = runstates.ABORTING

		self._log.info("[{}] Case aborted".format(self.name))

	def reset(self):
		if self.state not in [runstates.READY, runstates.PAUSED, runstates.FINISHED,
								runstates.FAILED, runstates.ABORTED]:
			raise Exception("Illegal action '{}' for state '{}'".format("reset", self.state))

		self._reset()

		# FIXME update component state in the database
		self.state = runstates.READY

		self._log.info("[{}] Case reset".format(self.name))

	def reload(self):
		self._log.info("[{}] Case reloaded".format(self.name))

		raise Exception("Unimplemented")

	# --------------------------------------------

	@property
	def started(self):
		return self.root_node.started

	@property
	def finished(self):
		return self.root_node.finished

	@property
	def elapsed(self):
		if self.root_node.started is not None and self.root_node.finished is not None:
			return self.root_node.finished - self.root_node.started
		else:
			return None

	def component(self, cname):
		"Returns a component by its cname. If the component doesn't exist raises an exception."

		if cname not in self._component_by_cname:
			raise Exception("Module not found: %s" % cname)

		return self._component_by_cname[cname]

	@property
	def task_count_by_state(self):
		return self.root_node.task_count_by_state

	def workitems(self, task_cname):
		"Returns an iterator of work-items for the task. Task can be a TaskNode or a task cname"

		session = db.Session()

		task = session.query(db.Task).filter(db.Task.case_id == self.id).filter(db.Task.cname == task_cname).first()
		if task is None:
			session.close()
			return []

		q = session.query(db.WorkItem).filter(db.WorkItem.case_id == self.id).filter(db.WorkItem.task_id == task.id)

		session.close()
		return q

	@property
	def storages(self):
		return [platform.storage.get_container(self.container_name) for platform in self.platforms]

	#TODO
	"""
	def task_logs(self, module_id, task_index):
		if self._storage.logs.exist(self.name, module_id, task_index):
			return self._storage.logs.query(self.name, module_id, task_index)

		task = self.task(module_id, task_index)
		if task.job_id is None:
			raise Exception("Task has not been submited yet: %s" % task.cname)

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

	def to_element(self, e = None):
		if e is None:
			e = Data.element()

		e["name"] = self.name
		e["state"] = str(self.state)
		e["conf"] = self.conf

		#FIXME
		self.root_node.update_tasks_count_by_state()
		self.root_node.update_component_count_by_state()
		self.root_node.to_element(e.element("root"))

		return e

	def __repr__(self):
		sb = []
		self.repr_level(sb, 0)
		return "".join(sb)

	def repr_level(self, sb, level):
		sb += [self._INDENT * level, "Case ", self.name, "\n"]
		level += 1
		sb += [self._INDENT * level, "State: ", str(self._state), "\n"]
		self.root_node.repr_level(sb, level)
		return level

# TODO: http://stackoverflow.com/questions/2405590/how-do-i-override-getattr-in-python-without-breaking-the-default-behavior
class SynchronizedCase(Synchronizable):
	def __init__(self, engine, case):
		Synchronizable.__init__(self, engine._lock)

		self.__engine = engine
		self.__case = case;

	def __getattr__(self, name):
		if name in ["name", "title", "state", "created", "started", "finished", "elapsed", "storages"]:
			return getattr(self.__case, name)
		else:
			raise AttributeError(name)

	@property
	@synchronized
	def task_count_by_state(self):
		return self.__case.task_count_by_state

	@property
	def tasks(self):
		return [ReadOnlyProxy(t) for t in self.__case.tasks]

	@synchronized
	def workitems(self, task):
		return self.__case.workitems(task)

	@synchronized
	def start(self):
		self.__case.start()

	@synchronized
	def pause(self):
		self.__case.pause()

	@synchronized
	def abort(self):
		self.__case.abort()

	@synchronized
	def reset(self):
		self.__case.reset()

	@synchronized
	def reload(self):
		self.__case.reload()
		
	@synchronized
	def to_element(self, e = None):
		return self.__case.to_element(e)

	def __repr__(self):
		return repr(self.__case)