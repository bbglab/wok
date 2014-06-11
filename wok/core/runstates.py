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

class RunState(object):
	def __init__(self, id, title, symbol):
		self.id = id
		self.title = title
		self.symbol = symbol

	def __eq__(self, other):
		return isinstance(other, RunState) and self.id == other.id

	def __hash__(self):
		return hash(self.id)

	def __str__(self):
		return self.title

	def __repr__(self):
		return "{}({})".format(self.title, self.id)

class UndefinedState(Exception):
	def __init__(self, id=None, symbol=None, title=None):
		if id is not None:
			Exception.__init__(self, "Undefined state id: {}".format(id))
		elif symbol is not None:
			Exception.__init__(self, "Undefined state symbol: {}".format(symbol))
		elif title is not None:
			Exception.__init__(self, "Undefined state title: {}".format(title))
		else:
			Exception.__init__(self, "Undefined state")

# Primary states

READY = RunState(1, "ready", "RD")
WAITING = RunState(2, "waiting", "W")
RUNNING = RunState(3, "running", "R")
PAUSED = RunState(4, "paused", "P")
ABORTING = RunState(5, "aborting", "AG")
FINISHED = RunState(6, "finished", "F")
RETRY = RunState(7, "retry", "RT")
FAILED = RunState(8, "failed", "E")
ABORTED = RunState(9, "aborted", "A")
UNKNOWN = RunState(9, "unknown", "U")

STATES = [READY, WAITING, RUNNING, PAUSED, ABORTING, FINISHED, RETRY, FAILED, ABORTED, UNKNOWN]

TERMINAL_STATES = [FINISHED, FAILED, ABORTED]

# Sub states

JOB_CREATED = RunState(10, "job_created", "JC")
LOGS_RETRIEVAL = RunState(11, "logs_retrieval", "LR")
JOINING = RunState(12, "joining", "J")

SUBSTATES = [JOB_CREATED, LOGS_RETRIEVAL, JOINING]

# -----------------------------------

__ID_MAP = {}
__SYMBOL_MAP = {}
__TITLE_MAP = {}

for s in STATES + SUBSTATES:
	__ID_MAP[s.id] = s
	__SYMBOL_MAP[s.symbol] = s
	__TITLE_MAP[s.title] = s

def from_title(title):
	if title not in __TITLE_MAP:
		raise UndefinedState(title=title)
	return __TITLE_MAP[title]

def from_id(id):
	if id not in __ID_MAP:
		raise UndefinedState(id=id)
	return __ID_MAP[id]

def from_symbol(symbol):
	if symbol not in __SYMBOL_MAP:
		raise UndefinedState(symbol=symbol)
	return __SYMBOL_MAP[symbol]