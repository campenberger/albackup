import unittest
import os
import sys
import tempfile
import shutil
import json
import copy
from sqlalchemy.util import pickle
from mock import patch,MagicMock

_baseDir=os.path.abspath(os.path.join(os.path.dirname(__file__),'..'))
if _baseDir not in sys.path:
    sys.path.insert(0,_baseDir)

from albackup.dump import Dump
from albackup import ObjectDef

class ListWithCopy(list):

	def copy(self):
		return copy.copy(self)

	def add(self,obj):
		self.append(obj)

class TestDump(unittest.TestCase):

	def setUp(self):
		super(TestDump,self).setUp()
		self.backup_dir=tempfile.mkdtemp(prefix='testdump_backup_dir')
		self.cache_dir=tempfile.mkdtemp(prefix='testdump_cache_dir')
		self.engine=MagicMock()
		self.dmp=Dump(self.backup_dir, self.cache_dir, self.engine,'the_database','my_server')
		self.dmp.con=MagicMock()

	def tearDown(self):
		shutil.rmtree(self.backup_dir)
		shutil.rmtree(self.cache_dir)
		super(TestDump,self).tearDown()

	@patch('albackup.dump.os.makedirs')
	@patch('albackup.dump.os.path.exists')
	def testConstractor(self,exists,makedirs):
		exists.return_value=False
		
		Dump(self.backup_dir, self.cache_dir, self.engine,'the_database','my_server')

		expectedDir=os.path.join(self.backup_dir,"the_database@my_server-")
		self.assertTrue( exists.call_args[0][0].startswith(expectedDir) )
		self.assertTrue( makedirs.call_args[0][0].startswith(expectedDir) )
		
	def test_get_meta_data_cached(self):
		meta_data_file=os.path.join(self.cache_dir,'the_database@my_server.pickle')
		with open(meta_data_file,"w") as fh:
			meta="<pickled meta data>"
			pickle.dump(meta,fh)

		self.assertEqual(
			"<pickled meta data>",
			self.dmp.get_meta_data()
		)

	@patch('albackup.dump.pickle')
	@patch('albackup.dump.sa.MetaData')
	def test_get_meta_data_reflected(self,MetaData,pickle):
		meta=MagicMock()
		MetaData.return_value=meta

		self.assertEqual(meta, self.dmp.get_meta_data())

		meta.reflect.assert_called_once_with(bind=self.engine)
		meta_data_file=os.path.join(self.cache_dir,'the_database@my_server.pickle')
		self.assertEqual(pickle.dump.call_args[0][0],meta)


	def test_backup_tables_multiple_tables(self):
		tables={
			'table1': MagicMock(**{'select.return_value': 'select from table1'}),
			'table2': MagicMock(**{'select.return_value': 'select from table2'})
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res1=MagicMock(**{'fetchmany.side_effect': ['block1',[]]})
		res2=MagicMock(**{'fetchmany.side_effect': ['block2',[]]})
		self.dmp.con.execute=MagicMock(side_effect=[res1,res2])
		
		self.dmp.backup_tables()

		self.assertTrue(os.path.exists(os.path.join(self.dmp.backup_dir,'table2.pickle')))
		self.assertTrue(os.path.exists(os.path.join(self.dmp.backup_dir,'table2.pickle')))

		self.dmp.con.execute.any_calls('select from table1')
		self.dmp.con.execute.any_calls('select from table2')

		res1.close.assert_called_once_with()
		res2.close.assert_called_once_with()

	def test_backup_tables_table_with_multiple_blocks(self):
		tables={
			'table1': MagicMock(**{'select.return_value': 'select from table1'})
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res1=MagicMock(**{'fetchmany.side_effect': ['block1','block2','block3',[]]})
		self.dmp.con.execute=MagicMock(return_value=res1)
		
		self.dmp.backup_tables()

		self.dmp.con.execute.assert_called_once_with('select from table1')

		with open(os.path.join(self.dmp.backup_dir,'table1.pickle'),'rb') as fh:
			l=fh.readline()
			self.assertEqual('block1',pickle.loads(fh.read(int(l))))
			l=fh.readline()
			self.assertEqual('block2',pickle.loads(fh.read(int(l))))
			l=fh.readline()
			self.assertEqual('block3',pickle.loads(fh.read(int(l))))
			l=fh.readline()
			self.assertEqual('EOF',l)

	@patch('albackup.dump.sa.Index')
	def test_fix_indexes_with_included_columns_no_change_required(self,Index):
		ix1=MagicMock()
		ix1.name='ix1'
		ix1.columns=['c1','c2','c3']
		tables={
			'table1': MagicMock(indexes=ListWithCopy([ix1]))
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res_object_id=MagicMock(**{'fetchone.return_value': ('oid17',)})
		res_typedef=MagicMock(**{
			#						  ix.type_desc,ix.is_unique,ix.is_primary_key,ix.is_unique_constraint
			'fetchone.return_value': ('??',        False,       False,            False)
		})
		res_included_columns=MagicMock(**{'fetchall.return_value': []})
		self.dmp.con.execute=MagicMock(side_effect=[res_object_id,res_typedef,res_included_columns])

		self.dmp.fix_indexes_with_included_columns()

		self.assertFalse(Index.called)
		self.assertEqual([ix1], tables['table1'].indexes)


	@patch('albackup.dump.sa.Index')
	def test_fix_indexes_with_included_columns_cluserted_index(self,Index):
		new_index=MagicMock()
		Index.return_value=new_index

		ix1=MagicMock()
		ix1.name='ix1'
		ix1.columns=['c1','c2','c3']
		tables={
			'table1': MagicMock(indexes=ListWithCopy([ix1]))
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res_object_id=MagicMock(**{'fetchone.return_value': ('oid17',)})
		res_typedef=MagicMock(**{
			#						  ix.type_desc,ix.is_unique,ix.is_primary_key,ix.is_unique_constraint
			'fetchone.return_value': ('CLUSTERED', False,       False,            False)
		})
		res_included_columns=MagicMock(**{'fetchall.return_value': []})
		self.dmp.con.execute=MagicMock(side_effect=[res_object_id,res_typedef,res_included_columns])

		self.dmp.fix_indexes_with_included_columns()

		Index.assert_called_once_with('ix1', 'c1', 'c2', 'c3', mssql_clustered=True)
		self.assertEqual([new_index], tables['table1'].indexes)

	@staticmethod
	def _makeColumn(ix):
			ret=MagicMock()
			ret.name='c{}'.format(ix)
			return ret

	@patch('albackup.dump.sa.Index')
	def test_fix_indexes_with_included_columns_with_included_columns(self,Index):
		new_index=MagicMock()
		Index.return_value=new_index
		
		ix1=MagicMock()
		ix1.name='ix1'
		ix1.columns=[TestDump._makeColumn(ix) for ix in range(1,4)]
		tables={
			'table1': MagicMock(indexes=ListWithCopy([ix1]))
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res_object_id=MagicMock(**{'fetchone.return_value': ('oid17',)})
		res_typedef=MagicMock(**{
			#						  ix.type_desc,ix.is_unique,ix.is_primary_key,ix.is_unique_constraint
			'fetchone.return_value': ('??',        False,       False,            False)
		})
		res_included_columns=MagicMock(**{'fetchall.return_value': [('c2',)]})
		self.dmp.con.execute=MagicMock(side_effect=[res_object_id,res_typedef,res_included_columns])

		self.dmp.fix_indexes_with_included_columns()

		Index.assert_called_once_with('ix1', ix1.columns[0], ix1.columns[2], mssql_include=['c2'])
		self.assertEqual([new_index], tables['table1'].indexes)

	@patch('albackup.dump.sa.Index')
	def test_fix_indexes_with_included_columns_unique_with_included_columns(self,Index):
		new_index=MagicMock()
		Index.return_value=new_index
		
		ix1=MagicMock()
		ix1.name='ix1'
		ix1.columns=[TestDump._makeColumn(ix) for ix in range(1,4)]
		tables={
			'table1': MagicMock(indexes=ListWithCopy([ix1]))
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		res_object_id=MagicMock(**{'fetchone.return_value': ('oid17',)})
		res_typedef=MagicMock(**{
			#						  ix.type_desc,ix.is_unique,ix.is_primary_key,ix.is_unique_constraint
			'fetchone.return_value': ('??',        True,       False,            False)
		})
		res_included_columns=MagicMock(**{'fetchall.return_value': [('c2',)]})
		self.dmp.con.execute=MagicMock(side_effect=[res_object_id,res_typedef,res_included_columns])

		self.dmp.fix_indexes_with_included_columns()

		Index.assert_called_once_with('ix1', ix1.columns[0], ix1.columns[2], mssql_include=['c2'], unique=True)
		self.assertEqual([new_index], tables['table1'].indexes)

	@patch('albackup.dump.sa.schema.PrimaryKeyConstraint')
	def test_fix_primary_key_order(self,PrimaryKeyConstraint):
		pk=MagicMock()
		pk.name='my_pk'

		columns={
			'c1': 'Column1',
			'c2': 'Column2',
			'c3': 'Column3'
		}
		tables={
			'table1': MagicMock(primary_key=pk,table_name='my_table',columns=columns)
		}
		self.dmp.info['meta']=MagicMock(tables=tables)

		pks=[
			('0?', '1?', '2?', 'c3', 3),
			('0?', '1?', '2?', 'c1', 1),
			('0?', '1?', '2?', 'c2', 2)
		]
		res=MagicMock(**{'fetchall.return_value':pks})
		self.dmp.con.execute=MagicMock(return_value=res)

		self.dmp.fix_primary_key_order()

		self.dmp.con.execute.assert_called_once_with("exec sp_pkeys 'table1'")
		res.close.assert_called_once_with()
		PrimaryKeyConstraint.assert_called_once_with('Column1', 'Column2', 'Column3', name='my_pk')


	def test_get_object_definitions(self):
		res1=MagicMock(**{
			'fetchall.return_value': [ ('obj1',), ('obj2',)]
		})
		res2=MagicMock(**{
			'fetchall.return_value': [ ('defintion',), ('for',), ('object1',)]
		})
		res3=MagicMock(**{
			'fetchall.return_value': [ ('defintion',), ('for',), ('object2',)]
		})
		self.dmp.con.execute=MagicMock(side_effect=[res1,res2,res3])

		self.assertEqual(
			[	ObjectDef(name='obj1', defintion='defintionforobject1', dependencies=None),
				ObjectDef(name='obj2', defintion='defintionforobject2', dependencies=None)
			],
			self.dmp._get_object_definitions('select db objects')
		)

	def test_get_object_dependencies(self):
		res=MagicMock(**{
			'fetchall.return_value': [
				('schema1','depency1'),
				('schema2','depency2')
			]	
		})
		self.dmp.con.execute=MagicMock(return_value=res)

		self.assertEqual(
			[	('schema1','depency1'),
				('schema2','depency2')
			],
			self.dmp._get_object_dependencies('obj1')
		)
		self.dmp.con.execute.assert_called_once_with(
			"SELECT DISTINCT referenced_schema_name, referenced_entity_name FROM sys.dm_sql_referenced_entities('dbo.obj1', 'OBJECT');"
		)


	def test_get_views(self):
		self.dmp._get_object_definitions=MagicMock(return_value=[
			ObjectDef('view1','defintion1',None),
			ObjectDef('view2','defintion2',None)
		])
		self.dmp._get_object_dependencies=MagicMock(side_effect=[
			[ ('dbo','t1') ],
			[ ('','t2'), ('dbo','tig1'), ('dbo','func1')]
		])
		self.dmp. _order_view_by_dependencies=lambda x: x

		self.assertEqual(
			[ 	ObjectDef(name='view1', defintion='defintion1', dependencies=['t1']),
				ObjectDef(name='view2', defintion='defintion2', dependencies=['t2', 'tig1', 'func1'])
			],
			self.dmp.get_views()
		)


	def test_get_views_with_external_dependency(self):
		self.dmp._get_object_definitions=MagicMock(return_value=[
			ObjectDef('view1','defintion1',None),
			ObjectDef('view2','defintion2',None)
		])
		self.dmp._get_object_dependencies=MagicMock(side_effect=[
			[ ('xyz','t1') ],
			[ ('','t2'), ('dbo','tig1'), ('dbo','func1')]
		])
		self.dmp. _order_view_by_dependencies=lambda x: x

		with self.assertRaises(Exception):
			self.dmp.get_views()


	def _create_db_objects(self):
		self.dmp.info['meta']=MagicMock(tables={
			't1': MagicMock(), 't2': MagicMock(), 't3': MagicMock()
		})
		
		self.dmp.info['functions']=[
			ObjectDef('f1',None,None),
			ObjectDef('f2',None,None)
		]
		self.dmp.info['procedures']=[
			ObjectDef('p1',None,None),
			ObjectDef('p2',None,None),
		]

	def test_order_view_by_dependencies(self):
		self._create_db_objects()
		views=[
			ObjectDef('v1',None,[]),
			ObjectDef('v2',None,['v3','v4','t1']),
			ObjectDef('v4',None,['t1','f1']),
			ObjectDef('v3',None,['t2','f1','p2'])
		]

		self.assertEqual(
			[	ObjectDef(name='v1', defintion=None, dependencies=[]),
				ObjectDef(name='v4', defintion=None, dependencies=['t1', 'f1']),
				ObjectDef(name='v3', defintion=None, dependencies=['t2', 'f1', 'p2']),
				ObjectDef(name='v2', defintion=None, dependencies=['v3', 'v4', 't1'])
			],
			self.dmp._order_view_by_dependencies(views)
		)


	def test_order_view_by_dependencies_circular(self):
		self._create_db_objects()
		views=[
			ObjectDef('v1',None,['v2','t1']),
			ObjectDef('v2',None,['v4','p1','t2']),
			ObjectDef('v4',None,['v1','t3','f2'])
		]
		with self.assertRaises(Exception):
			self.dmp._order_view_by_dependencies(views)

	def test_order_view_by_dependencies_missing(self):
		self._create_db_objects()
		views=[
			ObjectDef('v1',None,['t99'])
		]
		with self.assertRaises(Exception):
			self.dmp._order_view_by_dependencies(views)

	def test_get_procedures(self):
		self.dmp._get_object_definitions=MagicMock(
			return_value=[ ObjectDef('p1',None,None), ObjectDef('p2',None,None) ]
		)

		self.assertEqual(
			[ ObjectDef('p1',None,None), ObjectDef('p2',None,None) ],
			self.dmp.get_procedures()
		)
		self.dmp._get_object_definitions.assert_called_once_with(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE'"
		)

	def test_get_functions(self):
		self.dmp._get_object_definitions=MagicMock(
			return_value=[ ObjectDef('p1',None,None), ObjectDef('p2',None,None) ]
		)

		self.assertEqual(
			[ ObjectDef('p1',None,None), ObjectDef('p2',None,None) ],
			self.dmp.get_functions()
		)
		self.dmp._get_object_definitions.assert_called_once_with(
			"select routine_name from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION'"
		)

	def test_get_triggers(self):
		self.dmp._get_object_definitions=MagicMock(
			return_value=[ ObjectDef('t1','... on "db"."schema"."table" .....',None), ObjectDef('t2',"",None) ]
		)

		self.assertEqual(
			[ ObjectDef('t1','... on "schema"."table" .....',None), ObjectDef('t2',"",None) ],
			self.dmp.get_triggers()
		)
		self.dmp._get_object_definitions.assert_called_once_with(
			"select o.name from sysobjects o where type='TR';"
		)

	def test_finish_backup(self):
		self.dmp.finsih_backup()
		self.assertNotEqual("",self.dmp.info['finished'])
		self.assertTrue(os.path.join(self.backup_dir,"_metadata.pickle"))

if __name__=="__main__":
    unittest.main()