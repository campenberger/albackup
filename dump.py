import sqlalchemy as sa
import logging
import os
import pytz
import sqlalchemy.sql.expression as ex
from datetime import datetime
from sqlalchemy.sql import func
from collections import namedtuple
from sqlalchemy.util import pickle,byte_buffer


BLOCK_SIZE=100
backup_dir='backup'


db_user='sa'
db_password='gQjrrdp7iK8jvcME'
db_server='rds-td-stage.lexington-solutions.com'
db_port=1433
# db_name='stuyvesant'
db_name='reporting'

_getLogger=logging.getLogger

ObjectDef=namedtuple('ObjectDef',('name','defintion','dependencies'),verbose=False)

def backup_tables(meta,backup_dir,con):
	logger=_getLogger('backup_tables')
	for (table_name,table) in meta.tables.iteritems():
		file_name=os.path.join(backup_dir,'{}.pickle'.format(table_name))

		logger.info('Fetch data from %s',table_name) 
		res=con.execute(table.select())
		rows=res.fetchmany(BLOCK_SIZE)

		with open(file_name,'wb') as fh:
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


def _get_object_definitions(con,sql):
	logger=_getLogger('_get_object_definitions')
	logger.debug('Getting all object names from %s',sql)
	res=con.execute(sql)
	names=[ r[0] for r in res.fetchall() ]
	res.close()

	ret=[]
	for v in names:
		logger.debug('Fetching defintion of %s',v)
		res=con.execute("exec sp_helptext '%s'" % v)
		vdef=''.join( [r[0] for r in res.fetchall()] )
		res.close()
		ret.append( ObjectDef(v,vdef,None) )

	return ret


def get_views(con,meta):
	logger=_getLogger('get_views')
	logger.info('Retrieving all views')

	views=_get_object_definitions(con, "select o.name from sysobjects o where type='V';" )

	# determine the dependencies of each view
	new_views=[]
	for (i,view) in enumerate(views):
		logger.debug('Fetching dependencies for %s',view.name)
		trans=con.begin()
		res=con.execute(
			"SELECT DISTINCT referenced_schema_name, referenced_entity_name FROM sys.dm_sql_referenced_entities('dbo.%s', 'OBJECT');" % view.name
		)
		deps=res.fetchall()
		res.close()
		trans.commit()
		if any(map(lambda x: x[0] and x[0]!=u'dbo',deps)):
			msg='Unable to handle dependencies in other schema for {}'.format(view.name)
			logger.error(msg)
			raise Exception(msg)
		new_views.append( ObjectDef(view.name,view.defintion,map(lambda x: x[1],deps)) )
		logger.debug("   Got: %s",",".join(new_views[i].dependencies))


	# sort the views, based on their dependencies
	already_defined={ name: table for (name,table) in meta.tables.iteritems() }
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

	return ordered_views

def get_procedures(con):
	logger=_getLogger('get_procedures')

	logger.info('Retrieving all procuedures')
	return _get_object_definitions(
		con,
		"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE'"
	)


def get_functions(con):
	logger=_getLogger('get_functions')

	logger.info('Retrieving all functions')
	return _get_object_definitions(
		con,
		"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION'"
	)

def get_triggers(con):
	logger=_getLogger('get_triggers')
	logger.info('Retrieving all triggers')

	return _get_object_definitions(con, "select o.name from sysobjects o where type='TR';" )	


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
logger=logging.getLogger()


# engine=sa.create_engine('mssql+pyodbc://{}:{}@{}'.format(db_user,db_password,dsn_name))
engine=sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
	db_user,db_password,db_server,db_port,db_name
),deprecate_large_types=True)
con=engine.connect()

# either read cached meta data or reflect it from DB
# db_name=dsn_name
pickle_name='{}.pickle'.format(db_name)
if os.path.exists(pickle_name):
	meta=pickle.load(open(pickle_name,'rb'))
	logger.info('reflected metadata read from %s',pickle_name)
else:
	logger.info('reflecting the database meta data - this will take some time...')
	meta=sa.MetaData()
	meta.reflect(bind=engine)
	logger.info('reflected database')
	pickle.dump(meta, open(pickle_name,'wb'))
	logger.info('refelected metadata chached in %s',pickle_name)

if not os.path.exists(backup_dir):
	os.mkdir(backup_dir)
	logger.info('Backup dir %s created',backup_dir)


backup_info={
	'meta': meta,
	'started': datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
}

# iterate over tables and back them up
# backup_tables(meta,backup_dir,con)

# import pdb
# pdb.set_trace()


# get all the database objects
backup_info['views']=get_views(con,meta)
# backup_info['procedures']=get_procedures(con)
# backup_info['functions']=get_functions(con)
# backup_info['triggers']=get_triggers(con)

# write meta data to backup dir
backup_info['finished']=datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
file_name=os.path.join(backup_dir,'_metadata.pickle')
with open(file_name,'wb') as fh:
	pickle.dump(backup_info,fh)
logger.info('Meta data written to %s',file_name)