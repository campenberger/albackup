import argparse
import logging
import json
import sqlalchemy as sa

from .dump import Dump
from .restore import Restore
from . import Password


if __name__ == '__main__':
	parser=argparse.ArgumentParser("python -m albackup")
	parser.add_argument('mode',metavar='MODE',choices=('dump','restore','chg-password'), help="mode of operation (dump or restore,chg-password)")
	parser.add_argument('--cfg','-c',dest='cfg_file',default='albackup.json', help="Configuration for dump or restore operation")
	parser.add_argument('--meta-cache',default=None, help="Allow caching of database meta data")
	parser.add_argument('--backup-dir',default='backup',help="Target directory for backups")
	parser.add_argument('--debug','-d',action="store_true",default=False,help="Run in debug mode")
	args=parser.parse_args()

	logging.basicConfig(
		level=logging.DEBUG if args.debug else logging.INFO,
		format="%(asctime)s:%(name)-20s:%(levelname)-7s:%(message)s" if args.debug else "%(asctime)s: %(message)s"
	)
	logging.getLogger('sqlalchemy.engine').setLevel(
		logging.INFO if args.debug else logging.ERROR
	)
	logger=logging.getLogger()

	cfg=None
	with open(args.cfg_file,'r') as fh:
		cfg=json.load(fh)
		logger.info('Read configuration from %s',args.cfg_file)

	if args.mode!='chg-password':
		p=Password(args.cfg_file,cfg)
		pw=p.password

		logger.info('Database configuration:')
		logger.info('   user    : %s',cfg['db_user'])
		logger.info('   password: %s','*'*len(pw))
		logger.info('   server  : %s',cfg['db_server'])
		logger.info('   port    : %d',cfg['db_port'])
		logger.info('   db      : %s',cfg['db_name'])
		engine=sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
			cfg['db_user'],
			pw,
			cfg['db_server'],
			cfg['db_port'],
			cfg['db_name']
		),deprecate_large_types=True)
		logger.info('SQLAlchemy engine created.')

	if args.mode=='dump':
		dump=Dump(args.backup_dir, args.meta_cache, engine, cfg['db_name'], cfg['db_server'])
		dump.run()
		logger.info('Dump finished')

	elif args.mode=='restore':
		if not cfg['allow_restore']:
			raise Exception('Configuration file prohibits restore')
		enable_ri_check=cfg['enable_ri_check']
			
		restore=Restore(args.backup_dir,engine)
		restore.run()
		if enable_ri_check:
			restore.changeRIChecks(off=False)
		else:
			logger.info('RI checks where left off')
		logger.info('Restore finished')

	elif args.mode=='chg-password':
		pw=Password(args.cfg_file, cfg)
		pw.change()

	else:
		argparse.error("Invalid program mode")