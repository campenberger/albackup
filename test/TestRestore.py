import unittest
import os
import sys
import tempfile
import shutil
from mock import patch,MagicMock,mock_open,call
from sqlalchemy.util import pickle,byte_buffer
import sqlalchemy as sa


_baseDir=os.path.abspath(os.path.join(os.path.dirname(__file__),'..'))
if _baseDir not in sys.path:
    sys.path.insert(0,_baseDir)


from albackup.restore import Restore
from albackup import ObjectDef

def _breakpoint():
	import pdb
	pdb.set_trace()

class TestRestore(unittest.TestCase):

	def setUp(self):
		super(TestRestore,self).setUp()
		self.backup_dir=tempfile.mkdtemp(prefix='testdump_backup_dir')
		self.engine=MagicMock()
		self.maxDiff=65535

	def tearDown(self):
		shutil.rmtree(self.backup_dir)
		super(TestRestore,self).tearDown()

	def testConstructor(self):
		res=self._newRestore({'started': 'now', 'finished': 'a little later'})
		self.assertEqual(
			{'finished': 'a little later', 'started': 'now'},
			res.info
		)

	def test_getTablesWithLargeColumnTypes(self):
		meta=sa.MetaData()
		tables={
			't1': sa.Table('t1', meta, sa.Column('c1',sa.Integer) ),
			't2': sa.Table(
				't2', 
				meta, 
				sa.Column('c1',sa.Integer), 
				sa.Column('c2',sa.TEXT), 
				sa.Column('c3',sa.sql.sqltypes.NVARCHAR('max'))
			)
		}

		restore=self._newRestore({'meta': meta})

		ret=restore.getTablesWithLargeColumnTypes()
		self.assertEqual(
			str({
				't1': [],
				't2': [tables['t2'].columns['c2'], tables['t2'].columns['c3']]
			}), 
			str(ret)
		)

	def test_fixTextColumns(self):
		meta=sa.MetaData()

		sa.Table(
			't1', 
			meta, 
			sa.Column('c1',sa.Integer),
			sa.Column('c2',sa.TEXT(2147483647))
		)
		sa.Table(
			't2',
			meta,
			sa.Column('c3',sa.dialects.mssql.NTEXT(1073741823))
		)

		restore=self._newRestore({'meta': meta})
		restore.fixTextColumns()

		# the actual change of the type is hard to verify, so we only
		# check tat the right columns were detected and trust the type
		# changed happened as well
		self.assertEqual(
			{u't1.c2': True, u't2.c3': True},
			restore.suspect_columns
		)

	def test_createSchema(self):
		meta=sa.MetaData()
		restore=self._newRestore({'meta': meta})
		restore.con=MagicMock()
		with patch.object(restore,'_drop_views') as _drop_views:
			with patch.object(restore.info['meta'],'drop_all') as drop_all:
				with patch.object(restore.info['meta'],'create_all') as create_all:
					restore.createSchema()
					_drop_views.assert_called_once_with()
					drop_all.assert_called_once_with(restore.con)
					create_all.assert_called_once_with(restore.con)

	def test_changeRIChecks_on(self):
		restore=self._newRestore({})
		restore.con=MagicMock()
		restore.changeRIChecks(False)
		restore.con.execute.assert_called_once_with('EXEC sp_msforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all"')

	def test_changeRIChecks_off(self):
		restore=self._newRestore({})
		restore.con=MagicMock()
		restore.changeRIChecks(True)
		restore.con.execute.assert_called_once_with('EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"')

	def test_drop_views(self):
		v1=ObjectDef('v1','view 1',None)
		v2=ObjectDef('v2','view 2',None)
		restore=self._newRestore({
			'views': [v1,v2]
		})
		restore.con=MagicMock()
		restore._drop_views()
		self.assertEqual(
			call("IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE table_name= 'v2') DROP VIEW v2"),
			restore.con.execute.mock_calls[0]
		)
		self.assertEqual(
			call("IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE table_name= 'v1') DROP VIEW v1"),
			restore.con.execute.mock_calls[1]
		)

	def test_restore(self):
		restore=self._newRestore({})
		restore.info['meta']=MagicMock(tables={'t1': MagicMock})
		restore._largeColumns={'t1':[]}
		restore._getPrimaryKeyColumns=MagicMock(return_value=[])
		restore._insertBlock=MagicMock()
		restore._insertBlockWithLargeColumns=MagicMock()

		self._create_backup_file('t1',3)

		restore.import_tables()

		self.assertEqual(3,len(restore._insertBlock.mock_calls))
		self.assertFalse(restore._insertBlockWithLargeColumns.called)

	def test_restore_large_columns(self):
		restore=self._newRestore({})
		restore.info['meta']=MagicMock(tables={'t1': MagicMock})
		lc=MagicMock()
		lc.name='c1'
		restore._largeColumns={'t1':[lc]}
		restore._getPrimaryKeyColumns=MagicMock(return_value=['pk1'])
		restore._insertBlock=MagicMock()
		restore._insertBlockWithLargeColumns=MagicMock()

		self._create_backup_file('t1',3)

		restore.import_tables()

		self.assertEqual(3,len(restore._insertBlockWithLargeColumns.mock_calls))
		self.assertFalse(restore._insertBlock.called)

	def test_restore_large_columns_but_no_pks(self):
		restore=self._newRestore({})
		restore.info['meta']=MagicMock(tables={'t1': MagicMock})
		lc=MagicMock()
		lc.name='c1'
		restore._largeColumns={'t1':[lc]}
		restore._getPrimaryKeyColumns=MagicMock(return_value=[])
		restore._insertBlock=MagicMock()
		restore._insertBlockWithLargeColumns=MagicMock()

		self._create_backup_file('t1',3)

		restore.import_tables()

		self.assertEqual(3,len(restore._insertBlock.mock_calls))
		self.assertFalse(restore._insertBlockWithLargeColumns.called)

	def test_restore_large_columns_but_composite_pks(self):
		restore=self._newRestore({})
		restore.info['meta']=MagicMock(tables={'t1': MagicMock})
		lc=MagicMock()
		lc.name='c1'
		restore._largeColumns={'t1':[lc]}
		restore._getPrimaryKeyColumns=MagicMock(return_value=['pk1','pk2'])
		restore._insertBlock=MagicMock()
		restore._insertBlockWithLargeColumns=MagicMock()

		self._create_backup_file('t1',3)

		restore.import_tables()

		self.assertEqual(3,len(restore._insertBlock.mock_calls))
		self.assertFalse(restore._insertBlockWithLargeColumns.called)

	def test_restore_recycle_connection(self):
		restore=self._newRestore({})
		restore.info['meta']=MagicMock(tables={'t1': MagicMock})
		restore._largeColumns={'t1':[]}
		restore._getPrimaryKeyColumns=MagicMock(return_value=[])
		restore._insertBlock=MagicMock()
		restore._insertBlockWithLargeColumns=MagicMock()
		restore._recycleConnection=MagicMock()

		self._create_backup_file('t1',125)

		restore.import_tables()

		self.assertEqual(3, len(restore._recycleConnection.mock_calls))

	class ColumnsList(list):

		def __init__(self,*args,**kwargs):
			super(TestRestore.ColumnsList,self).__init__(*args,**kwargs)
			self._map={
				c.name: c
				for c in self
			}

		def __getitem__(self,key):
			if isinstance(key,int):
				return self[key]

			elif isinstance(key,basestring):
				return self._map[key]

			else:
				raise TypeError('Invalid key type {}'.format(key))

	def test_insertBlockWithLargeColumns(self):
		restore=self._newRestore({})
		restore.con=MagicMock()
		restore._insertBlock=MagicMock()

		pk=MagicMock()
		pk.name='pk'
		restore._getPrimaryKeyColumns=MagicMock(return_value=[pk])

		regular_column=MagicMock()
		regular_column.name='c1'

		def add_statement(col,value):
			return col.name+"='"+value+"'"

		long_column=MagicMock()
		long_column.name='long_column'
		long_column.__add__=add_statement
		restore._largeColumns={'t1': [long_column]}

		columns=TestRestore.ColumnsList([pk,regular_column,long_column])

		where=MagicMock(return_value='<update statement>')
		values=MagicMock(return_value=MagicMock(**{'where': where}))
		update_statement=MagicMock(return_value=MagicMock(**{'values': values}))

		table=MagicMock(**{
			'insert.return_value': '<insert statement>',
			'update': update_statement,
			'columns': columns
		})
		table.name='t1'

		rows=[
			{'pk': 1, 'c1': 'abcdef', 'long_column': 'abcdeghijl'},
			{'pk': 2, 'c1': 'ghijkl', 'long_column': 65535*'x'+1000*'y'},
			{'pk': 3, 'c1': 'mnopqr', 'long_column': 'abcdeghijl'}
		]

		restore._insertBlockWithLargeColumns(table, rows)

		restore._insertBlock.assert_called_once_with(
			table,
			[	{'long_column': 'abcdeghijl', 'pk': 1, 'c1': 'abcdef'}, 
				{'long_column': 'abcdeghijl', 'pk': 3, 'c1': 'mnopqr'}
			]
		)

		restore.con.execute.assert_has_calls([
			call('<insert statement>', {'long_column': u'', 'pk': 2, 'c1': 'ghijkl'}),
			call('<update statement>'),
			call('<update statement>')
		])
		self.assertEqual(2,len(where.mock_calls))
		values.assert_has_calls(
			[	call(long_column="long_column='"+65535*'x'+"'"),
				call(long_column="long_column='"+1000*'y'+"'")
			]
		)

	def test_insertBlock(self):
		restore=self._newRestore({})
		restore.con=MagicMock()

		table=MagicMock(**{
			'insert.return_value': '<insert statement>'
		})

		restore._insertBlock(table,'<rows>')

		restore.con.execute.assert_called_once_with('<insert statement>','<rows>')

	def test_getPrimaryKeyColumns(self):
		restore=self._newRestore({})

		pk1=MagicMock(primary_key=True)
		pk2=MagicMock(primary_key=True)
		c1=MagicMock(primary_key=False)
		c2=MagicMock(primary_key=False)
		table=MagicMock(**{'columns':[pk1,c1,pk2,c2]})

		self.assertEqual(
			[pk1,pk2],
			restore._getPrimaryKeyColumns(table)
		)

	def test_import_objects(self):
		restore=self._newRestore({})
		restore.info.update({
			'procedures':	"<procedures>",
			'functions':	"<functions>",
			'views':		"<views>",
			'triggers':		"<triggers>"
		})
		restore._import_object=MagicMock()

		restore.import_objects()

		restore._import_object.assert_has_calls([
			call("if exists (select * from information_schema.routines where routine_schema='dbo' and routine_type='PROCEDURE' and routine_name='%s') drop procedure %s", '<procedures>'),
	 		call("if exists (select * from information_schema.routines where routine_schema='dbo' and routine_type='FUNCTION' and routine_name='%s') drop function %s", '<functions>'),
 			call(None, '<views>'),
 			call("if exists (select * from sysobjects o where type='TR' and name='%s')drop trigger %s", '<triggers>')
 		])

 	def test_import_object(self):
 		restore=self._newRestore({})
 		restore.con=MagicMock()

 		objs=[
 			ObjectDef('o1', 'create object 1', None),
 			ObjectDef('o2', 'create object 2', None)
 		]

 		restore._import_object('if exists(%s) drop %s',objs)

 		restore.con.execute.assert_has_calls([
 			call('if exists(o1) drop o1'),
			call('create object 1'),
			call('if exists(o2) drop o2'),
			call('create object 2')
 		])


	def _create_backup_file(self,table_name,blocks):
		file_name=os.path.join(self.backup_dir,'{}.pickle'.format(table_name))
		
		buf=byte_buffer()
		pickle.dump("a block of data....",buf)
		buf=buf.getvalue()

		with open(file_name,'wb') as fh:
			for i in xrange(0,blocks):
				fh.write('{}\n'.format(len(buf)))
				fh.write(buf)
			fh.write('EOF')	

	def _newRestore(self,info=None):
		if not info: info={}

		from StringIO import StringIO
		fh=StringIO()
		pickle.dump(info,fh)
		fh.seek(0)
		
		with patch('albackup.restore.open') as _open:
			_open.return_value=fh
			fh.__enter__=MagicMock(return_value=fh)
			fh.__exit__=MagicMock()
			ret=Restore(self.backup_dir,self.engine)
			_open.assert_called_once_with(os.path.join(self.backup_dir,'_metadata.pickle'), 'rb')

		return ret


if __name__=="__main__":
    unittest.main()