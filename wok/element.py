# DEPRECATED -------------------------

from wok.config import Data

class DataFactory(Data):

	@staticmethod
	def create_element(data = None, key_sep=_DEFAULT_KEY_SEP): ### DEPRECATED
		print "WARN: Data.create_element is deprecated in favour of Data.element"
		return Data.element(data, key_sep=key_sep)

	@staticmethod
	def create_list(data = None, key_sep=_DEFAULT_KEY_SEP): ### DEPRECATED
		print "WARN: Data.create_list is deprecated in favour of Data.list"
		return Data.list(data, key_sep=key_sep)

	@staticmethod
	def from_native(obj, key_sep=_DEFAULT_KEY_SEP):
		"""
		Converts a python dictionary or list to a DataElement or DataList element respectively.

		# Example: converting a Dictionary
		>>> e = Data.from_native({'a': {'b': [1, 2]}})
		>>> print e #doctest: +NORMALIZE_WHITESPACE
		{
		  a = {
			b = [
			  1
			  2
			]
		  }
		}

		# Example: converting a List
		>>> l = Data.from_native([1, 2, 3])
		>>> print l
		[1, 2, 3]

		"""
		if isinstance(obj, dict):
			return DataElement(obj, key_sep)
		elif isinstance(obj, list):
			return DataList(obj, key_sep)
		else:
			return obj