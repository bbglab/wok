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

class ExitCode(object):

	__CODE_MAP = {}

	@staticmethod
	def from_code(code):
		if code is None:
			return None
		return ExitCode.__CODE_MAP[code] if code in ExitCode.__CODE_MAP else ExitCode(code, "")

	def __init__(self, code, title):
		self.code = code
		self.title = title

		self.__CODE_MAP[code] = self

	def __eq__(self, other):
		return isinstance(other, ExitCode) and self.code == other.code

	def __hash__(self):
		return hash(self.code)

	def __str__(self):
		return self.title

	def __repr__(self):
		if self.title is not none:
			return "{} ({})".format(self.title, self.code)
		else:
			return "({})".format(self.code)

UNKNOWN = ExitCode(200, "Unknown")
WAITING_EXCEPTION = ExitCode(201, "Exception while waiting")
TASK_EXCEPTION = ExitCode(202, "Exception while running")
RUN_THREAD_EXCEPTION = ExitCode(203, "Exception while executing")
ABORTED = ExitCode(204, "Aborted")
