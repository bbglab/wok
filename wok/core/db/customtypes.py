import json

import sqlalchemy.types as types

from wok.config.data import Data
from wok.core import runstates
from wok.core import exit_codes

class JSON(types.TypeDecorator):

	impl = types.Text

	def process_bind_param(self, value, dialect):
		return json.dumps(value)

	def process_result_value(self, value, dialect):
		return json.loads(value)

	def copy(self):
		return JSON(self.impl.length)

class Config(types.TypeDecorator):

	impl = types.Text

	def process_bind_param(self, value, dialect):
		return json.dumps(value.to_native()) if value is not None else None

	def process_result_value(self, value, dialect):
		return Data.create(json.loads(value)) if value is not None else None

	def copy(self):
		return Config(self.impl.length)

class RunState(types.TypeDecorator):

	impl = types.String

	def process_bind_param(self, value, dialect):
		return value.symbol if value is not None else None

	def process_result_value(self, value, dialect):
		return runstates.from_symbol(value) if value is not None else None


class ExitCode(types.TypeDecorator):

	impl = types.Integer

	def process_bind_param(self, value, dialect):
		return value.code if value is not None and isinstance(value, exit_codes.ExitCode) else value

	def process_result_value(self, value, dialect):
		return exit_codes.ExitCode.from_code(value) if value is not None else None