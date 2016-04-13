import logging
import sqlalchemy as sa
import os
import re
from sqlalchemy.util import pickle,byte_buffer
from sqlalchemy.dialects.mssql import NTEXT

from . import ObjectDef,DumpRestoreBase,loggerFactory,transaction



_getLogger=loggerFactory('restore')

class Restore(DumpRestoreBase):

	def __init__(self,backup_dir,engine):
		super(Restore,self).__init__(backup_dir,engine)

		file_name=os.path.join(self.backup_dir,'_metadata.pickle')
		with open(file_name,'rb') as fh:
			self.info=pickle.load(fh)
			_getLogger('Restore').info('Meta data read from %s',file_name)


	def fixTextColumns(self):
		logger=_getLogger('fixTextColumns')
		self.suspect_columns={}
		for tab in self.meta.tables:
			table=self.meta.tables[tab]
			for col in table.columns:
				if isinstance(col.type, sa.TEXT):
					table.columns[col.name].type=sa.TEXT(collation=u'SQL_Latin1_General_CP1_CI_AS')
					self.suspect_columns["%s.%s" % (tab, col.name)]=True
					logger.debug('Correctd column %s.%s',tab,col.name)

				elif isinstance(col.type, NTEXT):
					table.columns[col.name].type=NTEXT()
					self.suspect_columns["%s.%s" % (tab, col.name)]=True
					logger.debug('Correctd column %s.%s',tab,col.name)

	def createSchema(self):
		logger=_getLogger('createSchema')
		with transaction(self.con):
			self._drop_views()

			logger.info("Deleting tables ....")
			self.meta.drop_all(self.con)

			logger.info('Re-creating tables ....')
			self.meta.create_all(self.con)

	def changeRIChecks(self,off):
		logger=_getLogger('turnOffRIChecks')
		with transaction(self.con):
			if off:
				logger.info('Turn off RI checks')
				self.con.execute('EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"')
			else:
				logger.info('Turn on RI checks')
				self.con.execute('EXEC sp_msforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all"')


	def _drop_views(self):
		_getLogger('_drop_views').info('Dropping views')
		for v in reversed(self.views):
			self.con.execute("IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE table_name= '%s') DROP VIEW %s" % (v.name,v.name))


	def import_tables(self):
		logger=_getLogger('import_tables')
		logger.info('Importing tables')
		for (table_name,table) in self.meta.tables.iteritems():
			file_name=os.path.join(self.backup_dir,'{}.pickle'.format(table_name))

			logger.info('Restore data for table %s',table_name)
			logger.debug('   reading content from %s',file_name)
			with open(file_name,'rb') as fh:
				l=fh.readline()
				while l and l!='EOF':
					l=int(l)
					logger.debug('Importing block with %d bytes', l)

					buf=fh.read(l)
					rows=pickle.loads(buf)
					with transaction(self.con):
						try:
							self.con.execute(table.insert(),rows)
						except:
							logger.exception("Error inserting rows into %s:",table_name)
							logger.error("Dumping rows:")
							for r in rows:
								logger.error("   {}".format(r))
							raise

					l=fh.readline()  

	def import_objects(self):
		logger=_getLogger('import_objects')
		objects=(
			(self.views,		None,	"Views"),
			
			(self.procedures, 	"if exists (select * from information_schema.routines where routine_schema='dbo' "+\
				"and routine_type='PROCEDURE' and routine_name='%s') "+\
				"drop procedure %s",
				"Procedures"
			),
			
			(self.functions,	"if exists (select * from information_schema.routines where routine_schema='dbo' "+\
				"and routine_type='FUNCTION' and routine_name='%s') "+\
				"drop function %s",
				"Functions"
			),
			
			(self.triggers,		"if exists (select * from sysobjects o where type='TR' and type='%s')"+\
				"drop trigger %s",
				"Triggers"
			)
		)
		for obj in objects:
			logger.info('Importing %s',obj[2])
			self._import_object(obj[1],obj[0])



	def _import_object(self,check_and_delete,objs):
		logger=_getLogger('_import_object')
		crappy_backslash=re.compile(r'(\[.*?)\x92(.*?])')
		for v in objs:
			logger.debug('Recreating object %s',v[0])
			with transaction(self.con):
				if check_and_delete:
					self.con.execute(check_and_delete % (v[0],v[0]))

				sql=v[1]
				self.con.execute(sql)





