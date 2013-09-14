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

from native import NativeCommmandBuilder
from wok.core.errors import UnknownCommandBuilder

__CMD_BUILDERS = {
	"native" : NativeCommmandBuilder()
}

def create_command_builder(name):
	if name is None:
		name = "native"
	
	if name not in __CMD_BUILDERS:
		raise UnknownCommandBuilder(name)

	return __CMD_BUILDERS[name]