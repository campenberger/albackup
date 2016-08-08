import unittest
import os
import tempfile
import json
from mock import patch,MagicMock

from albackup import loggerFactory,transaction,execute_resultset,DumpRestoreBase,Password

class TestLoggerFactory(unittest.TestCase):

	# @patch('albackup.logging')
	def testLoggerFactory(self):
		getLogger=loggerFactory('TestInit')

		l0=getLogger()
		l1=getLogger('l1')
		l2=getLogger('l1','l1')

		self.assertEqual('albackup.TestInit',l0.name)
		self.assertEqual('albackup.TestInit.l1',l1.name)
		self.assertEqual('albackup.TestInit.l1.l1',l2.name)

class TransactionException(Exception): pass

class TestTransaction(unittest.TestCase):

	def testCommit(self):
		trans=MagicMock()
		con=MagicMock(**{'begin.return_value': trans})
		with transaction(con):
			pass

		con.begin.assert_called_once_with()
		trans.commit.assert_called_once_with()

	def testRollback(self):
		trans=MagicMock()
		con=MagicMock(**{'begin.return_value': trans})
		with self.assertRaises(TransactionException):
			with transaction(con):
				raise TransactionException()
			trans.rollback.assert_called_once_with()


class TestExecuteResultset(unittest.TestCase):

	def testExecute(self):
		res=MagicMock()
		con=MagicMock(**{'execute.return_value': res})

		with execute_resultset(con,'sql','args','kwargs') as result:
			self.assertEqual(result,res)

		con.execute.assert_called_once_with('sql','args','kwargs')
		res.close.assert_called_once_with()


class TestDumpRestoreBase(unittest.TestCase):

	def setUp(self):
		super(TestDumpRestoreBase,self).setUp()
		self.con=MagicMock()
		self.engine=MagicMock(**{'connect.return_value':self.con})
		self.dump_restore=DumpRestoreBase('./backups',self.engine)

	def testConstructor(self):
		self.assertEqual(self.con, self.dump_restore.con)
		self.assertIn('started',self.dump_restore.info)

	def testInfoProperties(self):
		self.dump_restore.info.update({
			'meta': 		'meta',
			'finished':		'finish time',
			'views':		'list of views',
			'procedures':	'list of procedures',
			'functions':	'list of functions',
			'triggers':		'list of triggers'
		})
		self.assertNotEqual('',self.dump_restore.started)
		self.assertEqual('meta', self.dump_restore.meta)
		self.assertEqual('finish time', self.dump_restore.finished)
		self.assertEqual('list of views', self.dump_restore.views)
		self.assertEqual('list of procedures', self.dump_restore.procedures)
		self.assertEqual('list of functions', self.dump_restore.functions)
		self.assertEqual('list of triggers', self.dump_restore.triggers)


class TestPassword(unittest.TestCase):

	def setUp(self):
		super(TestPassword,self).setUp()
		self.cfg_file=tempfile.NamedTemporaryFile(delete=False)
		self.cfg_file.close()
		self.cfg={
			'db_user': 'user',
			'db_server': 'server',
			'db_password': '/lbhLsvkCPECAAAAlIEfdjcrQHA=' # p2
		}
		self.pw=Password(self.cfg_file.name, self.cfg)

	def tearDown(self):
		os.remove(self.cfg_file.name)

	@patch('albackup.sys.stdin')
	def testChange_no_exising_pw(self,stdin):
		self.cfg['db_password']=None
		
		stdin.readline=MagicMock(side_effect=('p17\n','p17\n'))
		self.pw.change()

		self.assertEqual('p17',self.pw.password)

		with open(self.cfg_file.name) as fh:
			save_cfg=json.load(fh)
			self.assertIn('db_password', save_cfg)
			self.assertNotEqual("", save_cfg['db_password'])

	@patch('albackup.sys.stdin')
	def testChange_existing_pw(self,stdin):
		stdin.readline=MagicMock(side_effect=('p2\n','p17\n','p17\n'))
		self.pw.change()

		self.assertEqual('p17',self.pw.password)

		with open(self.cfg_file.name) as fh:
			save_cfg=json.load(fh)
			self.assertIn('db_password', save_cfg)
			self.assertNotEqual("", save_cfg['db_password'])

	@patch('albackup.sys.stdin')
	def testChange_exising_wrong(self,stdin):
		stdin.readline=MagicMock(side_effect=('w\n','w\n','w\n'))
		
		self.assertFalse(self.pw.change())

		self.assertEqual('p2',self.pw.password)

		with open(self.cfg_file.name) as fh:
			self.assertEqual("",fh.read())

	@patch('albackup.sys.stdin')
	def testChange_confirm_1_failure(self,stdin):
		stdin.readline=MagicMock(side_effect=('p2\n','p17\n','p18\n','p19\n','p19\n'))
		self.pw.change()

		self.assertEqual('p19',self.pw.password)

		with open(self.cfg_file.name) as fh:
			save_cfg=json.load(fh)
			self.assertIn('db_password', save_cfg)
			self.assertNotEqual("", save_cfg['db_password'])

	@patch('albackup.sys.stdin')
	def testChange_confirm_3_failures(self,stdin):
		stdin.readline=MagicMock(side_effect=('p2\n','p17\n','p18\n','p19\n','p20\n','p21\n','p22\n'))
		self.assertFalse(self.pw.change())

		self.assertEqual('p2',self.pw.password)

		with open(self.cfg_file.name) as fh:
			self.assertEqual("",fh.read())