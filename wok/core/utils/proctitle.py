# This module requires py-setproctitle to work otherwise it fails silently

# https://code.google.com/p/py-setproctitle/

from threading import current_thread

def set_proc_title(title):
	try:
		pass
		#from setproctitle import setproctitle
		#setproctitle(title)
	except:
		pass

def set_thread_title(title=None):
	if title is None:
		title = current_thread().name
	else:
		title = "{}: {}".format(current_thread().name, title)

	set_proc_title(title)