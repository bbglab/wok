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

import re
import os.path
import sqlite3
#import time
from datetime import datetime, timedelta

from wok.core.utils.sql import BatchInsert

_LOG_LEVEL_ID = {
	"debug" : 1,
	"info" : 2,
	"warn" : 3,
	"error" : 4
}

_LOG_LEVEL_NAME = {
	0 : None,
	1 : "debug",
	2 : "info",
	3 : "warn",
	4 : "error"
}


# 2011-10-06 18:39:46,849 bfast_localalign-0000 INFO  : hello world
_LOG_RE = re.compile("^(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d),(\d\d\d) (.*) ?\[(DEBUG|INFO|WARN|ERROR) *\] (.*)$")

# 2011-10-06 18:39:46 +0200 bfast_localalign-0000 INFO  : hello world
#_LOG_RE = re.compile("^(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d:\d+) (.*) (DEBUG|INFO|WARN|ERROR) +: (.*)$")

class LogsDb(object):
	def __init__(self, path):
		self._path = path

		self._conn = None

	def open(self):
		create = not os.path.exists(self._path)
		self._conn = sqlite3.connect(self._path)
		if create:
			self.__create_db()

	def __create_db(self):
		c = self._conn.cursor()
		c.execute("""
		CREATE TABLE logs (
			module_id	TEXT,
			task_index	INTEGER,
			timestamp	TEXT,
			level		INTEGER,
			msg			TEXT
		)""")
		c.close()

	def __parse_line(self, line):
		m = _LOG_RE.match(line)
		if m:
			timestamp = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
			timestamp += timedelta(milliseconds=int(m.group(2)))
			name = m.group(3)
			level = m.group(4).lower()
			text = m.group(5).decode("utf-8", "replace")
		else:
			timestamp = None
			name = None
			level = None
			text = line.decode("utf-8", "replace")

		return (timestamp, level, name, text)

	def add(self, instance_name, module_id, task_index, stream):
		cur = self._conn.cursor()
		bi = BatchInsert(cur, "logs", ["module_id", "task_index", "timestamp", "level", "msg"], batch_size=1)

		#last_timestamp = None
		line = stream.readline()
		while len(line) > 0:
			timestamp, level, name, text = self.__parse_line(line)
		#if timestamp is None:
		#	timestamp = last_timestamp
		#last_timestamp = timestamp

			if timestamp is not None:
				#timestamp = time.mktime(timestamp.timetuple()) + 1e-6 * timestamp.microsecond
				timestamp = timestamp.strftime("%Y%m%d %H%M%S %f")
			level = _LOG_LEVEL_ID.get(level, 0)
			bi.insert(module_id, task_index, timestamp, level, text)

			line = stream.readline()

		bi.close()
		self._conn.commit()
		cur.close()

	#TODO convert into an iterator
	def query(self, instance_name, module_id, task_index):
		cur = self._conn.cursor()

		logs = []

		sql = ["SELECT * FROM logs"]

		cur.execute(" ".join(sql))

		log = cur.fetchone()
		while log is not None:
			timestamp = datetime.strptime(log[0], "%Y%m%d %H%M%S %f")
			ms = timestamp.microsecond / 1000
			timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S:{0:03}".format(ms))
			level = _LOG_LEVEL_NAME[log[1]].upper()
			logs += [(timestamp, level, log[2], log[3])]
			log = cur.fetchone()

		cur.close()
		return logs

	def close(self):
		if self._conn is not None:
			self._conn.close()
			self._conn = None