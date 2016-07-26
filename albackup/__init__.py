from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from datetime import datetime
import logging
import pytz

def _getLogger(*names):
	''' Utility method that returns a logger prefixed with 'albackup'
		and all given names, separate by '.'. For example

		_getLogger('common','util') is equivalent to

		logging.getLogger('albackup.common.util')
	'''
	name='.'.join(['albackup']+[n for n in names])
	return logging.getLogger(name)

def loggerFactory(name):
	''' Helper to generate the getLogger method for a module, where
		the first level is preset.

			gl=loggerFactory('test')
			logger=gl('class1')
			logger.name
			>>> 'albackup.test.class1'
	'''
	return partial(_getLogger,name)


ObjectDef=namedtuple('ObjectDef',('name','defintion','dependencies'),verbose=False)
''' simple tuple class for database objects '''

@contextmanager
def transaction(con):
	''' A context manager instance that wrapps database operations in a transaction
		and either commits the transaction or rolls it back in case of an exception.

			with transaction(con):
				con.execute('something bad')

	'''
	trans=con.begin()
	try:
		yield
		trans.commit()
	except:
		_getLogger('transaction').exception('Exception in transaction - rolling back')
		trans.rollback()
		raise

@contextmanager
def execute_resultset(con,sql,*args,**kwargs):
	''' A context manager that wraps the excution of a SQL statement with a close
		at the end of the operation
	
			with execute_resultset(con,'select ...') as res:
				while res.fetchrow():		
	'''
	res=con.execute(sql,*args,**kwargs)
	yield res
	res.close()


class DumpRestoreBase(object):
	''' Base class for the dump and restore operations to capture common information
		like the backup_directory, the sqlalchemy engine, the database conneciton in use
		and the backup meta data. It also has several convenience properties to access
		members of the backup meta data
	'''

	def __init__(self,backup_dir,engine):
		''' Constructor

			* backup_dir - the backup base directory
			* engine - SQLAlchemy database engine
		'''
		self.backup_dir=backup_dir
		self.engine=engine
		self.con=engine.connect()
		_getLogger('DumpRestoreBase').debug('Connected to database')
		self.info={
			'started': datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')		
		}

	@property
	def meta(self):
		''' Convenience property to retrieve the SQLAlchemy meta data from the backup
			info
		'''
		return self.info['meta']

	@property
	def started(self): 
		''' Convenience property to retrive the start time from the backup info
		'''
		return self.info['started']

	@property
	def finished(self):
		''' Convenience property to retrive the end time from the backup info
		'''
		return self.info['finished']

	@property
	def views(self): 
		''' Convenience property to retrive the list of ObjectDef objects with
			views from the backup info
		'''
		return self.info['views']

	@property
	def procedures(self): 
		''' Convenience property to retrive the list of ObjectDef objects with
			procedures from the backup info
		'''
		return self.info['procedures']

	@property
	def functions(self): 
		''' Convenience property to retrive the list of ObjectDef objects with
			functions from the backup info
		'''
		return self.info['functions']

	@property
	def triggers(self):
		''' Convenience property to retrive the list of ObjectDef objects with
			triggers from the backup info
		'''
		return self.info['triggers']


import sys
import base64
from Crypto.Cipher import Blowfish
from Crypto import Random
import struct

class Password(object):
	''' Class to blowfish encrypt a user password. It is used in the
		json configuration files to avoid user readable passwords.
	'''

	SECRET="So 1 Tag s0 wunderschoen wie heute"

	def __init__(self,cfg_file,cfg):
		''' Constructor:

			* cfg_file - name of the configuration file
			* cfg - dict object with the configuration from the file
		'''
		self.cfg_file=cfg_file
		self.cfg=cfg


	def change(self):
		''' Main method that verifies the current password, if applicable,
			and then propmps for a new password and then confirms it, before
			writing it back into the json configuration file.
		'''
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
		''' Convenience property to return the decrypted password from the
			configuration file
		'''
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