# ******************************************************************
# Copyright 2009, Universitat Pompeu Fabra
#
# Licensed under the Non-Profit Open Software License version 3.0
# ******************************************************************

PORT_MODE_IN = "in"
PORT_MODE_OUT = "out"

def create_port(mode, conf, storage):
	if "port/name" not in conf:
		raise Exception("Port missing name: %s" % repr(conf))

	name = str(conf["port/name"])

	data = storage.create_port_data_from_conf(conf)

	if mode == PORT_MODE_IN:
		return InPort(name, data)
	elif mode == PORT_MODE_OUT:
		return OutPort(name, data)
	else:
		raise Exception("Unknown port mode: %s" % mode)

class Port(object):
	def __init__(self, name, data):
		self.name = name
		self.data = data

	@property
	def mode(self):
		raise Exception("Unsupported method")
		
class InPort(Port):
	def __init__(self, name, data):
		Port.__init__(self, name, data)
		
		self._reader = self.data.reader()

	@property
	def mode(self):
		return PORT_MODE_IN

	def _open(self):
		self._reader = self.data.reader()

	def size(self):
		return self._reader.size()
		
	def __iter__(self):
		if self._reader is None:
			self._open()

		return iter(self._reader)

	def read(self, size = 1):
		if self._reader is None:
			self._open()
			
		return self._reader.read(size)

	def read_all(self):
		size = self.size()
		data = self.read(size)
		if data is None:
			data = []
		elif isinstance(data, list):
			return data
		else:
			return [data]

	def close(self):
		if self._reader is not None:
			self._reader.close()
			self._reader = None

class OutPort(Port):
	def __init__(self, name, data):
		Port.__init__(self, name, data)
		
		self._writer = self.data.writer()

	@property
	def mode(self):
		return PORT_MODE_OUT

	def _open(self):
		self._writer = self.data.writer()
		
	def write(self, data):
		if self._writer is None:
			self._open()

		self._writer.write(data)

	def close(self):
		if self._writer is not None:
			self._writer.close()
			self._writer = None

