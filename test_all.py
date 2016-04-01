import json
import logging
import argparse
import sqlalchemy as sa
import pyodbc

from albackup import loggerFactory
from albackup.dump import Dump
from albackup.restore import Restore

_getLogger=loggerFactory('test_all')

def create_engine(cfg):
	logger=_getLogger('create_engine')

	logger.info('Database configuration:')
	logger.info('   user    : %s',cfg['db_user'])
	logger.info('   password: %s','*'*len(cfg['db_password']))
	logger.info('   server  : %s',cfg['db_server'])
	logger.info('   port    : %d',cfg['db_port'])
	logger.info('   db      : %s',cfg['db_name'])
	
	return sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
		cfg['db_user'],
		cfg['db_password'],
		cfg['db_server'],
		cfg['db_port'],
		cfg['db_name']
	),deprecate_large_types=True)


class DatabaseRecreator(object):

	def __init__(self,cfg):
		logger=_getLogger('DatabaseRecreate')
		self.db=cfg['db_name']
		self.con=pyodbc.connect(
			'DRIVER={{FreeTDS}};TDS_Version=8.0;UID={};PWD={};SERVER={};PORT={};DATABASE={}'.format(
				cfg['db_user'],
				cfg['db_password'],
				cfg['db_server'],
				cfg['db_port'],
				'master'
			), 
			autocommit=True
		)
		logger.info('Created pydobc target to manage restore connection:')
		logger.info('   user    : %s',cfg['db_user'])
		logger.info('   password: %s','*'*len(cfg['db_password']))
		logger.info('   server  : %s',cfg['db_server'])
		logger.info('   port    : %d',cfg['db_port'])
		logger.info('   db      : %s','master')
			
	def recreate(self):
		c=self.con.cursor()

		c.execute("SELECT name FROM master.sys.databases WHERE name = N'{}'".format(self.db))
		if len(c.fetchall())>0:
			self.con.execute('drop database {}'.format(self.db))	
		c.close()

		self.con.execute('create database {}'.format(self.db))
		_getLogger('DatabaseRecreate').info('Database %s re-created',self.db)



parser=argparse.ArgumentParser("Test prog to backup restore all databases and comparing them")
parser.add_argument('--debug','-d',action="store_true",default=False,help="Run in debug mode")
args=parser.parse_args()

logging.basicConfig(
	level=logging.DEBUG if args.debug else logging.INFO,
	format="%(asctime)s:%(name)-40s:%(levelname)-7s:%(message)s" if args.debug else "%(asctime)s: %(message)s"
)
logging.getLogger('sqlalchemy.engine').setLevel(
	logging.INFO if args.debug else logging.ERROR
)
logger=_getLogger()

# load test.json
cfg=None
with open('test.json') as fh:
	cfg=json.load(fh)
	logger.info('Read configuration from test.json')

recreator=DatabaseRecreator(cfg['restore'])


# iterate over all configs
for test_cfg in cfg['databases']:
	logger.info('Starting Test for %s',test_cfg['name'])
	engine=create_engine(test_cfg)

	# run dump
	dump=Dump('./backup`', None, engine, test_cfg['db_name'], test_cfg['db_server'])
	dump.get_meta_data()
	dump.backup_tables()
	dump.get_views()
	dump.get_procedures()
	dump.get_functions()
	dump.get_triggers()
	dump.finsih_backup()
	logger.info('Dump finished. Backup in %s',dump.backup_dir)
	engine.dispose()

	# re-create database
	recreator.recreate()

	# now the restore
	backup_dir=dump.backup_dir
	engine=create_engine(cfg['restore'])
	assert cfg['restore']['allow_restore']
	enable_ri_check=cfg['restore']['enable_ri_check']
			
	restore=Restore(backup_dir,engine)
	restore.fixTextColumns()
	restore.createSchema()
	restore.changeRIChecks(off=True)
	restore.import_tables()
	restore.import_objects()
	if enable_ri_check:
		restore.changeRIChecks(off=False)
	else:
		logger.info('RI checks where left off')
	logger.info('Restore finished')
	engine.dispose()

# databases to add
# // sandbox
# // sandbox-reporting
# // astar
# // astar-reporting
# // astarstage
# // astarstage-reporting
# // td
# // revere