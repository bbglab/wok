#!/usr/bin/env python

###############################################################################
#
#    Copyright 2009-2013, Universitat Pompeu Fabra
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

import os
from datetime import datetime

from wok import logger
from wok.config.optionsconfig import OptionsConfig
from wok.config.data import Data
from wok.server import WokFlask, WokServer

import home

app = WokFlask(__name__)
app.register_blueprint(home.bp, url_prefix="/")

def main():
	conf = OptionsConfig()

	if "wok" not in conf:
		print("Missing wok configuration")
		exit(-1)

	conf.expand_vars()

	logger.initialize(conf.get("wok.logging"))

	log = logger.get_logger("wok.server.start")

	# run the server

	retcode = 0
	try:
		server = WokServer(conf, app)
		server.init()
		server.run()
	except Exception as e:
		log.exception(e)
		retcode = -1

	exit(retcode)

if __name__ == "__main__":
	main()
