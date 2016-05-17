import sqlalchemy as sa
import logging
import os
import pytz
import re
import sqlalchemy.sql.expression as ex
from datetime import datetime
from sqlalchemy.sql import func
from collections import namedtuple
from sqlalchemy.util import pickle,byte_buffer

from . import ObjectDef,loggerFactory,transaction,execute_resultset,DumpRestoreBase

BLOCK_SIZE=500

_getLogger=loggerFactory('Dump')

class Dump(DumpRestoreBase):

	def __init__(self,backup_dir,meta_data_dir,engine,db_name,db_server):
		super(Dump,self).__init__(backup_dir,engine)
		self.meta_data_dir=meta_data_dir
		self.db_name=db_name
		self.db_server=db_server

		self.backup_dir=os.path.join(
			backup_dir if backup_dir else '.',
			'{}@{}-{}'.format(db_name,db_server,datetime.utcnow().strftime('%Y%m%d-%H%M'))
		)
		if not os.path.exists(self.backup_dir):
			os.makedirs(self.backup_dir)
			_getLogger('Dump').info('Backup dir %s created',backup_dir)


	def run(self):
		self.get_meta_data()
		self.fix_primary_key_order()
		self.fix_indexes_with_included_columns()
		self.backup_tables()
		self.get_procedures()
		self.get_functions()
		self.get_triggers()
		self.get_views()
		self.finsih_backup()

	def get_meta_data(self):
		logger=_getLogger('get_meta_data')
		meta=None
		pickle_name=None
		if self.meta_data_dir:
			pickle_name=os.path.join(self.meta_data_dir,'{}@{}.pickle'.format(self.db_name,self.db_server))
			if os.path.exists(pickle_name):
				meta=pickle.load(open(pickle_name,'rb'))
				logger.info('Got reflected metadata read from %s',pickle_name)

		if meta is None:
			logger.info('Reflecting the database meta data - this will take some time...')
			meta=sa.MetaData()
			meta.reflect(bind=self.engine)
			logger.info('Reflected database')
			if pickle_name:
				pickle.dump(meta, open(pickle_name,'wb'))
				logger.info('Refelected metadata chached in %s',pickle_name)

		self.info['meta']=meta
		return meta

 	def backup_tables(self,):
		logger=_getLogger('backup_tables')
		meta=self.info['meta']

		for (table_name,table) in meta.tables.iteritems():
			file_name=os.path.join(self.backup_dir,'{}.pickle'.format(table_name))

			logger.info('Fetch data from %s',table_name) 
			with transaction(self.con):
				res=self.con.execute(table.select())

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

	def fix_indexes_with_included_columns(self):
		logger=_getLogger('fix_indexes_with_included_columns')
		for (table_name,table) in self.meta.tables.iteritems():
			if len(table.indexes)>0:
				with transaction(self.con):
					# get the object id of the table table
					schema=table.schema if table.schema else 'dbo'
					res=self.con.execute(
						'''select tab.object_id 
							from sys.schemas as sch
								join sys.tables as tab on sch.schema_id=tab.schema_id
							where sch.name='{}' and tab.name='{}'
						'''.format(schema,table_name)
					)
					object_id=res.fetchone()[0]
					res.close()
					logger.debug('Object id for table %s: %s',table_name,str(object_id))
					
					# we use a shallow copy of the indexes, because we will alter
					# the set while iterating over it. We have to do several things:
					# 
					# - check for clustered indexses and change the type def accordingly
					# - check for included columns and if there are any, recreate the index
					#   with the correct definition
					for ix in table.indexes.copy():
						new_ix_def={
							'required': False,
							'index_columns': ix.columns,
							'args': {}
						}

						logger.debug('Check defintion of index %s for clustered',ix.name)
						res=self.con.execute('''
							select ix.type_desc,ix.is_unique,ix.is_primary_key,ix.is_unique_constraint
							from sys.indexes as ix 
							where ix.object_id='{}' and ix.name='{}'
						'''.format(object_id,ix.name))
						type_def=res.fetchone()
						res.close()
						if type_def[0]=='CLUSTERED':
							logger.debug('Index %s is clustered',ix.name)
							new_ix_def['args']['mssql_clustered']=True
							new_ix_def['required']=True

						if type_def[1] and not (type_def[2] or type_def[3]):
							logger.debug('Index %s in unique, but not PK or unique constraint')
							new_ix_def['args']['unique']=True

						# check for included columns
						logger.debug('Getting included columns for index %s',ix.name)
						res=self.con.execute('''
							select col.name
							from sys.indexes as ix 
			  					join sys.index_columns as ixcol on ix.object_id=ixcol.object_id and ix.index_id=ixcol.index_id
			  					join sys.columns as col on ix.object_id=col.object_id and ixcol.column_id=col.column_id
							where ix.object_id='{}' and ix.name='{}'  and ixcol.is_included_column='true'
						'''.format(object_id,ix.name))
						included_columns=res.fetchall()
						res.close()
						if len(included_columns)>0:
							new_ix_def['args']['mssql_include']=map(lambda x: x[0],included_columns)
							new_ix_def['index_columns']=filter(lambda x: x.name not in included_columns, ix.columns)
							new_ix_def['required']=True							
						else:
							logger.debug("Index %s on %s has no included columns",table_name,ix.name)

						# redefine the index, if needed
						if new_ix_def['required']:
							table.indexes.remove(ix)
							new_ix=sa.Index(
								ix.name, 
								*new_ix_def['index_columns'],
								**new_ix_def['args']
							)
							table.indexes.add(new_ix)
			else:
				logger.info('Table %s has no indexes',table_name)

	def fix_primary_key_order(self):
		''' Fix the column sequencing of all pimary keys '''

		logger=_getLogger('fix_primary_key_order')
		for (table_name,table) in self.meta.tables.iteritems():
			if table.primary_key:
				logger.info("Checking primary for %s",table_name)

				with transaction(self.con):
					res=self.con.execute("exec sp_pkeys '{}'".format(table_name))
					pkeys=res.fetchall()
					res.close()
					pkeys.sort(key=lambda x: x[4])

					table.primary_key=sa.schema.PrimaryKeyConstraint(*[
						table.columns[r[3]]
						for r in pkeys
					],
					name=table.primary_key.name)
			else:
				logger.warn('No primary key for %s',table_name)


	def _get_object_definitions(self,sql):
		logger=_getLogger('_get_object_definitions')
		logger.debug('Getting all object names from %s',sql)

		with transaction(self.con):
			with execute_resultset(self.con, sql) as res:
				names=[ r[0] for r in res.fetchall() ]

			ret=[]
			for v in names:
				logger.debug('Fetching defintion of %s',v)
				with execute_resultset(self.con, "exec sp_helptext '%s'" % v ) as res:
					vdef=''.join( [r[0] for r in res.fetchall()] )
					ret.append( ObjectDef(v,vdef,None) )
		return ret

	def _get_object_dependencies(self,name):
		with transaction(self.con):
			sql="SELECT DISTINCT referenced_schema_name, referenced_entity_name FROM sys.dm_sql_referenced_entities('dbo.%s', 'OBJECT');" % name

			with execute_resultset(self.con, sql) as res:
				return res.fetchall()

	def get_views(self):
		logger=_getLogger('get_views')
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
			
			new_views.append( ObjectDef(view.name,view.defintion,map(lambda x: x[1].lower(),deps)) )
			logger.debug("   Got: %s",",".join(new_views[i].dependencies))


		# create list of already predefined objects
		already_defined={ name.lower(): table for (name,table) in self.meta.tables.iteritems() }
		already_defined.update({ o.name.lower(): o for o in self.functions})
		already_defined.update({ o.name.lower(): o for o in self.procedures})

		# sort the views, based on their dependencies
		ordered_views=[]
		while len(new_views)>0:
			logger.debug('Len new_views before: %d',len(new_views))
			remaining_views=[]
			for view in new_views:
				if all(map(lambda x: x in already_defined,view.dependencies)):
					already_defined[view.name.lower()]=view
					ordered_views.append(view)
				else:
					remaining_views.append(view)
			if len(new_views)==len(remaining_views):
				msg='No changes were made during ordering views'
				logger.error('No changes were made during ordering views')
				logger.error('Remaining views:\n%s',"\n".join([
					"%s: %s" % (
						x.name,
						",".join([y for y in x.dependencies])
					) 
					for x in remaining_views
					])
				)
				logger.error('Already defined:\n%s',
					'\n'.join(sorted(already_defined.keys()))
				)
				raise Exception(msg)
			new_views=remaining_views
			logger.debug('Len new_views after: %d',len(new_views))

		logger.debug('Order views: {}'.format([ x.name for x in ordered_views]))

		self.info['views']=ordered_views
		return ordered_views

	def get_procedures(self):
		logger=_getLogger('get_procedures')
		logger.info('Retrieving all procuedures')
		self.info['procedures']=self._get_object_definitions(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE'"
		)
		return self.info['procedures']


	def get_functions(self):
		logger=_getLogger('get_functions')
		logger.info('Retrieving all functions')
		self.info['functions']=self._get_object_definitions(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION'"
		)
		return self.info['functions']

	def get_triggers(self):
		logger=_getLogger('get_triggers')
		logger.info('Retrieving all triggers')
		self.info['triggers']=self._get_object_definitions("select o.name from sysobjects o where type='TR';" )

		regex=re.compile(r'on "(.+?)"\."(.+?)"\."(.+?)" ',re.I | re.U)

		def fix_view_name(objdef):
			text=regex.sub(r'on "\2"."\3" ',objdef.defintion)
			return ObjectDef(objdef.name, text, objdef.dependencies)

		self.info['triggers']=map(fix_view_name,self.info['triggers'])
		return self.info['triggers']

	def finsih_backup(self):
		logger=_getLogger('finsih_backup')
		self.info['finished']=datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
		file_name=os.path.join(self.backup_dir,'_metadata.pickle')
		with open(file_name,'wb') as fh:
			pickle.dump(self.info,fh)
		logger.info('Meta data written to %s',file_name)


