import argparse
import logging
import json
import sqlalchemy as sa

from .dump import Dump
from .restore import Restore

import sys
import base64
from Crypto.Cipher import Blowfish
from Crypto import Random
import struct

class Password(object):

	SECRET="So 1 Tag s0 wunderschoen wie heute"

	def __init__(self,cfg_file,cfg):
		self.cfg_file=cfg_file
		self.cfg=cfg


	def change(self):
		cnt=0
		while cnt<3:
			cnt=cnt+1
			print "Changing password for %s@%s" % (cfg['db_name'],cfg['db_server'])

			if 'db_password' in cfg and cfg['db_password']:
				print "Old Password: ",
				l=sys.stdin.readline()[:-1]
				if not self._check(l):
					print "Invalid password"
					continue


			print "New Password: ",
			p1=sys.stdin.readline()[:-1]

			print "Again: ",
			p2=sys.stdin.readline()[:-1]

			if p1!=p2:
				print "Passwords don't match!"
				continue

			self.cfg['db_password']=self._encrypt(p1)
			with open(self.cfg_file,'w') as fh:
				json.dump(self.cfg,fh,indent=3)
				print "Configuration file %s updated with new password" % self.cfg_file

			return
					
		print "Changing password failed!"

	@property
	def password(self):
	    return self._decrypt(self.cfg['db_password'])
	
	def _check(self,pw):
		old=self._decrypt(self.cfg['db_password'])
		return old==pw

	def _encrypt(self,pw):
		bs = Blowfish.block_size
		iv = Random.new().read(bs)
		cipher = Blowfish.new(self.SECRET, Blowfish.MODE_CBC, iv)

		pw=bytes(pw)
		plen = bs - divmod(len(pw),bs)[1]
		padding = [plen]*plen
		padding = struct.pack('b'*plen, *padding)
		blen=struct.pack('i',len(pw))
		raw=iv + blen + cipher.encrypt(pw + padding)
		return base64.b64encode(raw)


	def _decrypt(self,pw):
		pw=base64.b64decode(pw)
		bs = Blowfish.block_size
		isize=struct.calcsize('i')
		(iv,blen,pw)=(pw[0:bs], pw[bs:bs+isize], pw[bs+isize:])

		blen=struct.unpack('i',blen)[0]
		cipher = Blowfish.new(self.SECRET, Blowfish.MODE_CBC, iv)
		ret=cipher.decrypt(pw)
		return ret[0:blen]


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

	elif args.mode=='chg-password':
		pw=Password(args.cfg_file, cfg)
		pw.change()

	else:
		argparse.error("Invalid program mode")