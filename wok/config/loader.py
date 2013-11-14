import os
import json

from data import Data

class ConfigLoader(object):
	def __init__(self, path):
		self.path = path

	def load(self):
		try:
			with open(self.path, "r") as f:
				v = json.load(f)
			return Data.create(v)
		except Exception as e:
			from wok.logger import get_logger
			msg = ["Error loading configuration from ",
					self.path, ":\n\n", str(e), "\n"]
			get_logger(__name__).error("".join(msg))
			raise