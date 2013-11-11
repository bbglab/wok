from wok import logger as woklogger

from model import DB_VERSION, DbParam

__CHANGES = [
	[ # 2
	  	"ALTER TABLE cases ADD COLUMN started DATETIME",
		"ALTER TABLE cases ADD COLUMN finished DATETIME",
		"ALTER TABLE cases ADD COLUMN state VARCHAR",
		"ALTER TABLE cases ADD COLUMN project_name VARCHAR",
		"ALTER TABLE cases ADD COLUMN flow_name VARCHAR",
		"CREATE UNIQUE INDEX ix_cases_engine_name ON cases (engine_name)"
	]
]

def db_update(engine, session, logger=None):
	if logger is None:
		logger = woklogger.get_logger(__name__)

	version_param = session.query(DbParam).filter(DbParam.name == "version").first()
	if version_param is None:
		version_param = DbParam(name="version", value=1)
		session.add(version_param)

	if version_param.value < DB_VERSION:
		logger.info("Updating the server database ...")
		for i in range(version_param.value, DB_VERSION):
			logger.info("  {} --> {} ...".format(i, i + 1))
			sts = __CHANGES[i - 1]
			if isinstance(sts, basestring):
				sts = [sts]
			for sql in sts:
				logger.debug("    " + sql)
				engine.execute(sql)

	version_param.value = DB_VERSION

def db_init(engine, session, new_db, logger=None):
	if new_db:
		version_param = DbParam(name="version", value=DB_VERSION)
		session.add(version_param)
	else:
		db_update(engine, session, logger)
