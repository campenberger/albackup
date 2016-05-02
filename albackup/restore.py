import logging
import sqlalchemy as sa
import os
import re
from sqlalchemy.util import pickle,byte_buffer
from sqlalchemy.dialects.mssql import NTEXT,NVARCHAR

from . import ObjectDef,DumpRestoreBase,loggerFactory,transaction



_getLogger=loggerFactory('restore')

class Restore(DumpRestoreBase):

	def __init__(self,backup_dir,engine):
		super(Restore,self).__init__(backup_dir,engine)

		file_name=os.path.join(self.backup_dir,'_metadata.pickle')
		with open(file_name,'rb') as fh:
			self.info=pickle.load(fh)
			_getLogger('Restore').info('Meta data read from %s',file_name)

		self._getTablesWithLargeColumnTypes()


	def run(self):
		self.fixTextColumns()
		self.createSchema()
		self.changeRIChecks(off=True)
		self.import_tables()
		self.import_objects()

	def _getTablesWithLargeColumnTypes(self):

		def isLargeColumnType(col):
			type=col.type
			return isinstance(type,sa.TEXT) or (isinstance(type,sa.sql.sqltypes.NVARCHAR) and type.length=='max')

		self._largeColumns={
			tname: filter(isLargeColumnType,table.columns)
			for (tname,table) in self.meta.tables.iteritems()
		}
		return self._largeColumns


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

			large_columns=self._largeColumns[table_name]
			file_name=os.path.join(self.backup_dir,'{}.pickle'.format(table_name))

			logger.info('Restore data for table %s',table_name)
			logger.debug('   table has large columns: %s',','.join([c.name for c in large_columns]))
			logger.debug('   reading content from %s',file_name)
			cnt=100
			pks=self._getPrimaryKeyColumns(table)
			if len(large_columns)>0 and len(pks)!=1:
				logger.warn('Table %s with blobs has more or no primary key columns - falling back to block insert',table_name)
			with open(file_name,'rb') as fh:
				l=fh.readline()
				while l and l!='EOF':
					l=int(l)
					buf=fh.read(l)
					rows=pickle.loads(buf)
					logger.debug('Importing block with %d bytes and %d rows', l,len(rows))

					# freetds seems to have a bug, where the odbc connection after a number
					# of requests gets bad. So, we recyle the connection after a while
					if cnt>=50:
						logger.debug('Recyling connection')
						con=self.con
						self.con=self.engine.connect()
						con.invalidate()
						con.close()
						cnt=0
					else:
						cnt=cnt+1

					with transaction(self.con):
						if len(large_columns)>0 and len(pks)==1:
							self._insertBlockWithLargeColumns(table,rows)
						else:
							self._insertBlock(table,rows)
							

					l=fh.readline()  

	def _insertBlockWithLargeColumns(self,table,rows):
		logger=_getLogger('_insertBlockWithLargeColumns')
		large_columns=self._largeColumns[table.name]

		def hasLargeField(row):
			for c in large_columns:
				if row[c.name] and len(row[c.name])>65535:
					return True
			return False

		def insertRow(row):
			try:
				self.con.execute(table.insert(),row)
			except:
				logger.exception("Error inserting rows into %s:",table.name)
				raise

		def updateLargeColumn(pk,pk_value,col,value):
			logger.debug('Setting large value for column %s in row with pk %s',col.name,str(pk_value))
			while len(value)>0:
				chunk=value[0:65535]
				value=value[65535:]
				args={col.name: col+chunk}
				try:
					self.con.execute(table.update()\
						.values(**args)\
						.where(pk==pk_value))
				except:
					logger.exception('Error while setting large value for column %s in row with pk %s',col.name,str(pk_value))
					raise

		# get rows without and without large columns
		ok_rows=[]
		problem_rows=[]
		for row in rows:
			if hasLargeField(row):
				problem_rows.append(row)
			else:
				ok_rows.append(row)
		logger.debug('%d rows without large columns and %d rows with',len(ok_rows),len(problem_rows))

		# first the ones without problems
		self._insertBlock(table,ok_rows)
		
		# after finding the id key, we iterate over the rows 
		# for each row we create a map with the columns over 65k
		# before we set it to an emptt string in the row and 
		# insert. Afterwards we update the large values with chunks of
		# 65k each
		pk=self._getPrimaryKeyColumns(table)[0]
		logger.debug('Primary key: %s',pk.name)
		for row in problem_rows:
			pk_value=row[pk.name]
			logger.debug('Processing problem row pk=%s',str(pk_value))

			large_value_map={}
			new_row={}
			for col in table.columns:
				if col in large_columns and row[col.name] and len(row[col.name])>65535:
					large_value_map[col.name]=row[col.name]
					new_row[col.name]=u''
				else:
					new_row[col.name]=row[col.name]

			logger.debug('  -> inserting the row')
			insertRow(new_row)

			logger.debug('  -> setting the large columns')
			for (c,v) in large_value_map.iteritems():
				col=table.columns[c]
				updateLargeColumn(pk,pk_value,col,v)



	def _insertBlock(self,table,rows):
		try:
			self.con.execute(table.insert(),rows)
		except:
			logger.exception("Error inserting rows into %s:",table.name)
			logger.error("Dumping rows:")
			for r in rows:
				logger.error("   {}".format(r))
			raise		

	def _getPrimaryKeyColumns(self,table):
		return filter(lambda c: c.primary_key,table.columns)
		

	def import_objects(self):
		logger=_getLogger('import_objects')
		objects=(
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

			(self.views,		None,	"Views"),
			
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





