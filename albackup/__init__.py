from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from datetime import datetime
import logging
import pytz

def getLogger(*names):
	name='.'.join(['albackup']+[n for n in names])
	return logging.getLogger(name)

def loggerFactory(name):
	return partial(getLogger,name)

ObjectDef=namedtuple('ObjectDef',('name','defintion','dependencies'),verbose=False)

@contextmanager
def transaction(con):
	trans=con.begin()
	try:
		yield
		trans.commit()
	except:
		trans.rollback()
		getLogger('transaction').exception('Exception in transaction - rolling back')
		raise

@contextmanager
def execute_resultset(con,sql,*args,**kwargs):
	res=con.execute(sql,*args,**kwargs)
	yield res
	res.close()


class DumpRestoreBase(object):

	def __init__(self,backup_dir,engine):
		self.backup_dir=backup_dir
		self.engine=engine
		self.con=engine.connect()
		self.info={
			'started': datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')		
		}

	@property
	def meta(self): return self.info['meta']

	@property
	def started(self): return self.info['started']

	@property
	def finished(self): return self.info['finished']

	@property
	def views(self): return self.info['views']

	@property
	def procedures(self): return self.info['procedures']

	@property
	def functions(self): return self.info['functions']

	@property
	def triggers(self):  return self.info['triggers']