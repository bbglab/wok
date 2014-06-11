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

class MissingRequiredArgumentError(Exception):
	def __init__(self, key):
		Exception.__init__(self, "Missing required argument: '{}'".format(key))

class MissingRequiredArgumentsError(Exception):
	def __init__(self, keys):
		Exception.__init__(self, "Missing required arguments: {}".format(", ".join(keys)))

# Config

class MissingConfigParamError(Exception):
	def __init__(self, key):
		Exception.__init__(self, "Missing configuration parameter: '{}'".format(key))

class MissingConfigParamsError(Exception):
	def __init__(self, keys):
		Exception.__init__(self, "Missing configuration parameters: '{}'".format(", ".join(keys)))

class ConfigTypeError(Exception):
	def __init__(self, key, value=None):
		if value is None:
			Exception.__init__(self, "Wrong configuration type for '{}'".format(key))
		else:
			Exception.__init__(self, "Wrong configuration type for '{}': '{}'".format(key, value))

# Engine

class EngineAlreadyRunningError(Exception):
	pass

class InvalidStatusOperationError(Exception):
	def __init__(self, op, status):
		Exception.__init__(self, "Invalid operation '{}' for current status '{}'".format(op, status))

class EngineUninitializedError(Exception):
	pass

# Job manager

class UnknownJobManager(Exception):
	def __init__(self, name):
		Exception.__init__(self, "Unknown job manager: {}".format(name))


class UnknownJob(Exception):
	def __init__(self, job_id):
		Exception.__init__(self, "Unknown job id: {}".format(job_id))

# Command builder

class UnknownCommandBuilder(Exception):
	def __init__(self, name):
		Exception.__init__(self, "Unknown command builder: {}".format(name))


class MissingValueError(Exception):
	def __init__(self, name):
		Exception.__init__(self, "Missing required value: {}".format(name))


class LanguageError(Exception):
	def __init__(self, lang):
		Exception.__init__(self, "Unknown language: {}".format(lang))