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
from wok.core.errors import UnknownJobManager

from wok.jobs.mcore import McoreJobManager

__JOB_MANAGERS = {
	"mcore" : McoreJobManager
}

def __log(msg, level=None):
	from wok.logger import get_logger
	log = get_logger(name="wok.jobs")

	if level is None:
		import logging
		level = logging.WARNING
	log.log(level, msg)

try:
	from wok.jobs.saga import SagaJobManager
	__JOB_MANAGERS["saga"] = SagaJobManager
except ImportError:
	__log("The SAGA job manager can not be loaded.")
	__log("This manager is necessary only if you are going to run jobs in a cluster.")
	__log("To install it run the following command: pip install saga-python")

def create_job_manager(name, conf):
	if name is None or name == "default":
		name = "mcore"

	if name not in __JOB_MANAGERS:
		raise UnknownJobManager(name)

	conf["type"] = name

	return __JOB_MANAGERS[name](conf)
