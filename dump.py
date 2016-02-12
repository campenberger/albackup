import sqlalchemy as sa
import logging
import os
import pytz
import sqlalchemy.sql.expression as ex
from datetime import datetime
from sqlalchemy.sql import func
from collections import namedtuple
from sqlalchemy.util import pickle,byte_buffer

from albackup import ObjectDef,loggerFactory,transaction,execute_resultset,DumpRestoreBase

BLOCK_SIZE=500


db_user='sa'
db_password='gQjrrdp7iK8jvcME'
db_server='rds-td-stage.lexington-solutions.com'
db_port=1433
# db_name='stuyvesant'
db_name='reporting'

_getLogger=loggerFactory('dump')

class Dump(DumpRestoreBase):

	def __init__(self,backup_dir,meta_data_dir,engine):
		super(Dump,self).__init__(backup_dir,engine)
		self.meta_data_dir=meta_data_dir
		if not os.path.exists(self.backup_dir):
			os.mkdir(self.backup_dir)
			_getLogger('Dump').info('Backup dir %s created',backup_dir)


	def get_meta_data(self):
		logger=_getLogger('Dump','get_meta_data')
		pickle_name=os.path.join(self.meta_data_dir,'{}.pickle'.format(db_name))
		if os.path.exists(pickle_name):
			meta=pickle.load(open(pickle_name,'rb'))
			logger.info('Got reflected metadata read from %s',pickle_name)
		else:
			logger.info('Reflecting the database meta data - this will take some time...')
			meta=sa.MetaData()
			meta.reflect(bind=engine)
			logger.info('Reflected database')
			pickle.dump(meta, open(pickle_name,'wb'))
			logger.info('Refelected metadata chached in %s',pickle_name)

		self.info['meta']=meta
		return meta

 	def backup_tables(self,):
		logger=_getLogger('Dump','backup_tables')
		meta=self.info['meta']

		for (table_name,table) in meta.tables.iteritems():
			file_name=os.path.join(self.backup_dir,'{}.pickle'.format(table_name))

			logger.info('Fetch data from %s',table_name) 
			with transaction(con):
				res=con.execute(table.select())

				with open(file_name,'wb') as fh:
					rows=res.fetchmany(BLOCK_SIZE)
					
					while len(rows)>0:
						logger.debug("  Got %d rows - writing to backup file",len(rows))
						
						buf=byte_buffer()
						pickle.dump(rows,buf)
						buf=buf.getvalue()

						fh.write('{}\n'.format(len(buf)))
						fh.write(buf)

						rows=res.fetchmany(BLOCK_SIZE)
					fh.write("EOF")

				logger.info("Written backup to %s",file_name)
				res.close()


	def _get_object_definitions(self,sql):
		logger=_getLogger('Dump','_get_object_definitions')
		logger.debug('Getting all object names from %s',sql)

		with transaction(con):
			with execute_resultset(self.con, sql) as res:
				names=[ r[0] for r in res.fetchall() ]

			ret=[]
			for v in names:
				logger.debug('Fetching defintion of %s',v)
				with execute_resultset(con, "exec sp_helptext '%s'" % v ) as res:
					vdef=''.join( [r[0] for r in res.fetchall()] )
					ret.append( ObjectDef(v,vdef,None) )
		return ret

	def _get_object_dependencies(self,name):
		with transaction(self.con):
			sql="SELECT DISTINCT referenced_schema_name, referenced_entity_name FROM sys.dm_sql_referenced_entities('dbo.%s', 'OBJECT');" % name

			with execute_resultset(self.con, sql) as res:
				return res.fetchall()

	def get_views(self):
		logger=_getLogger('Dump','get_views')
		logger.info('Retrieving all views')

		views=self._get_object_definitions("select o.name from sysobjects o where type='V';" )

		# determine the dependencies of each view
		new_views=[]
		for (i,view) in enumerate(views):
			logger.debug('Fetching dependencies for %s',view.name)

			deps=self._get_object_dependencies(view.name)
			if any(map(lambda x: x[0] and x[0]!=u'dbo',deps)):
				msg='Unable to handle dependencies in other schema for {}'.format(view.name)
				logger.error(msg)
				raise Exception(msg)
			
			new_views.append( ObjectDef(view.name,view.defintion,map(lambda x: x[1],deps)) )
			logger.debug("   Got: %s",",".join(new_views[i].dependencies))


		# sort the views, based on their dependencies
		already_defined={ name: table for (name,table) in self.meta.tables.iteritems() }
		ordered_views=[]
		while len(new_views)>0:
			logger.debug('Len new_views before: %d',len(new_views))
			remaining_views=[]
			for view in new_views:
				if all(map(lambda x: x in already_defined,view.dependencies)):
					already_defined[view.name]=view
					ordered_views.append(view)
				else:
					remaining_views.append(view)
			if len(new_views)==len(remaining_views):
				raise Exception('No changes were made during ordering views')
			new_views=remaining_views
			logger.debug('Len new_views after: %d',len(new_views))

		logger.debug('Order views: {}'.format([ x.name for x in ordered_views]))

		self.info['views']=ordered_views
		return ordered_views

	def get_procedures(self):
		logger=_getLogger('Dump','get_procedures')
		logger.info('Retrieving all procuedures')
		self.info['procuedures']=self._get_object_definitions(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE'"
		)
		return self.info['procuedures']


	def get_functions(self):
		logger=_getLogger('Dump','get_functions')
		logger.info('Retrieving all functions')
		self.info['functions']=self._get_object_definitions(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION'"
		)
		return self.info['functions']

	def get_triggers(self):
		logger=_getLogger('Dump','get_triggers')
		logger.info('Retrieving all triggers')
		self.info['triggers']=self._get_object_definitions("select o.name from sysobjects o where type='TR';" )	
		return self.info['triggers']

	def finsih_backup(self):
		logger=_getLogger('Dump','finsih_backup')
		self.info['finished']=datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
		file_name=os.path.join(self.backup_dir,'_metadata.pickle')
		with open(file_name,'wb') as fh:
			pickle.dump(self.info,fh)
		logger.info('Meta data written to %s',file_name)


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
logger=logging.getLogger()


# engine=sa.create_engine('mssql+pyodbc://{}:{}@{}'.format(db_user,db_password,dsn_name))
engine=sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
	db_user,db_password,db_server,db_port,db_name
),deprecate_large_types=True)
con=engine.connect()

dump=Dump('backup','.',engine)

dump.get_meta_data()
dump.backup_tables()
dump.get_views()
dump.get_procedures()
dump.get_functions()
dump.get_triggers()

dump.finsih_backup()