import argparse
import logging
import json
import sqlalchemy as sa
from .dump import Dump

if __name__ == '__main__':
	parser=argparse.ArgumentParser("Description backup or restore database with SQLAlchemy")
	parser.add_argument('mode',metavar='MODE',choices=('dump','restore'), help="mode of operation (dump or restore)")
	parser.add_argument('--cfg','-c',dest='cfg_file',default='albackup.json', help="Configuration for dump or restore operation")
	parser.add_argument('--meta-cache',default=None, help="Allow caching of database meta data")
	parser.add_argument('--backup-dir',default='backup',help="Target directory for backups")
	args=parser.parse_args()

	logging.basicConfig(level=logging.INFO)
	logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
	logger=logging.getLogger()

	cfg=None
	with open(args.cfg_file,'r') as fh:
		cfg=json.load(fh)
		logger.info('Read configuration from %s',args.cfg_file)

	logger.info('Database configuration:')
	logger.info('   user    : %s',cfg['db_user'])
	logger.info('   password: %s','*'*len(cfg['db_password']))
	logger.info('   server  : %s',cfg['db_server'])
	logger.info('   port    : %d',cfg['db_port'])
	logger.info('   db      : %s',cfg['db_name'])
	engine=sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
		cfg['db_user'],
		cfg['db_password'],
		cfg['db_server'],
		cfg['db_port'],
		cfg['db_name']
	),deprecate_large_types=True)
	logger.info('SQLAlchemy engine created.')

	if args.mode=='dump':
		dump=Dump(args.backup_dir, args.meta_cache, engine, cfg['db_name'], cfg['db_server'])
		dump.get_meta_data()
		dump.backup_tables()
		dump.get_views()
		dump.get_procedures()
		dump.get_functions()
		dump.get_triggers()
		dump.finsih_backup()

	else:
		raise Exception("Not implemented yet")
