import logging
import sqlalchemy as sa
import os
from sqlalchemy.util import pickle,byte_buffer
from sqlalchemy.dialects.mssql import NTEXT

backup_dir='backup'

db_user='sa'
db_password='gQjrrdp7iK8jvcME'
db_server='rds-td-stage.lexington-solutions.com'
db_port=1433
db_name='cam_test'


_getLogger=logging.getLogger

def import_tables(meta,backup_dir,con):
	logger=_getLogger('import_tables')
	for (table_name,table) in meta.tables.iteritems():
		file_name=os.path.join(backup_dir,'{}.pickle'.format(table_name))

		logger.info('Restore data for table %s from %s',table_name,file_name)
		trans = con.begin()
		with open(file_name,'rb') as fh:
			l=fh.readline()
			while l and l!='EOF':
				l=int(l)
				print "Blocksize: %d" % l

				buf=fh.read(l)
				rows=pickle.loads(buf)
				# for row in rows:
				con.execute(table.insert(),rows)

				l=fh.readline()  
		#trans.rollback()
		trans.commit()

def _import_object(con,check_and_delete,objs):
	logger=_getLogger('_import_object')
	for v in objs:
		logger.debug('Recreating object %s',v[0])
		trans=con.begin()
		try:
			con.execute(check_and_delete % (v[0],v[0]))
			con.execute(v[1])

			trans.commit()
		except:
			# trans.rollback()
			raise


def import_views(view_defs,con):
	_import_object(
		con,
		"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE table_name= '%s') DROP VIEW %s",
		view_defs
	)

def import_procedures(objs,con):
	_import_object(
		con,
		"if exists (select * from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE' and routine_name='%s')"+\
			"drop procedure %s",
		objs
	)

def import_functions(objs,con):
	_import_object(
		con,
		"if exists (select * from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION' and routine_name='%s')"+\
			"drop function %s",
		objs
	)	

def import_triggers(objs,con):
	_import_object(
		con,
		"if exists (select * from sysobjects o where type='TR' and type='%s')"+\
			"drop trigger %s",
		objs
	)	

logging.basicConfig(level=logging.DEBUG)
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger=logging.getLogger()


engine=sa.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=FreeTDS&odbc_options="TDS_Version=8.0"'.format(
	db_user,db_password,db_server,db_port,db_name
),deprecate_large_types=True)
con=engine.connect()
logger.info('Connected to %s',db_name)

# read the meta data
file_name=os.path.join(backup_dir,'_metadata.pickle')
with open(file_name,'rb') as fh:
	backup_info=pickle.load(fh)
	logger.info('Meta data red from %s',file_name)
meta=backup_info['meta']

# do suergery on the text columns
suspect_columns={}
for tab in meta.tables:
	table=meta.tables[tab]
	for col in table.columns:
		if isinstance(col.type, sa.TEXT):
			table.columns[col.name].type=sa.TEXT(collation=u'SQL_Latin1_General_CP1_CI_AS')
			suspect_columns["%s.%s" % (tab, col.name)]=True

		elif isinstance(col.type, NTEXT):
			table.columns[col.name].type=NTEXT()
			suspect_columns["%s.%s" % (tab, col.name)]=True

# creating the schema
logger.info('Re-creating tables ....')
meta.drop_all(con)
meta.create_all(con)

# turn of RI checks
logger.info('Turn off RI checks')
trans = con.begin()
con.execute('EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"')
trans.commit()

# import data from all tables, incl warning if max data length is exceeded
logger.info('Restoring tables....')
import_tables(meta,backup_dir,con)

# re-create all the views
logger.info('Recreating views...')
import_views(backup_info['views'],con)
import_procedures(backup_info['procedures'],con)
import_functions(backup_info['functions'],con)
import_triggers(backup_info['triggers'],con)

# turn RI checking back on
logger.info('Turn RI checks back on')
trans = con.begin()
con.execute('EXEC sp_msforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all"')
trans.commit()
