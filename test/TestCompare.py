import unittest
import os
from mock import patch,MagicMock
from albackup.compare import DbCompare

_baseDir=os.path.abspath(os.path.join(os.path.dirname(__file__),'..'))

class TestDbCompare(unittest.TestCase):

	def setUp(self):
		super(TestDbCompare,self).setUp()
		ref_cfg={
			"name": "ref",
			"db_name": "ref_db_name",
			"db_user": "ref_user",
			"db_password": "ref_password",
			"db_server": "ref_server"
		}
		target_cfg={
			"name": "target",
			"db_name": "target_db_name",
			"db_user": "target_user",
			"db_password": "target_pw",
			"db_server": "target_server"
		}
		self.cmp=DbCompare(ref_cfg,target_cfg,"./sqlworkbench")

	def test_make_tempale(self):
		cmd_file=self.cmp._make_template()
		cmd_file.seek(0)
		sql=cmd_file.read().replace("\n","")

		self.assertIn(
			'WbCreateProfile  -name="ref"   -savePassword=true  -username="ref_user"  -password="ref_password"  -url="jdbc:jtds:sqlserver://ref_server/ref_db_name"  -driver=net.sourceforge.jtds.jdbc.Driver;',
			sql
		)
		self.assertIn(
			'WbCreateProfile  -name="target"   -savePassword=true  -username="target_user"  -password="target_pw"  -url="jdbc:jtds:sqlserver://target_server/target_db_name"  -driver=net.sourceforge.jtds.jdbc.Driver;',
			sql
		)
		
		expected='WbSchemaDiff   -referenceProfile="ref"  -targetProfile="target"  '+\
			'-file="{bd}/diffs/diff-ref.xml"  -includeIndex=true  -includeProcedures=true  '+\
			'-includeSequences=true  -includeTableGrants=true  -includePrimaryKeys=true  '+\
			'-includeForeignKeys=true  -includeViews=true  '+\
			'-styleSheet="{bd}/sqlworkbench/xslt/wbdiff2html.xslt"  -xsltOutput="{bd}/diffs/diff-ref.html";'
		expected=expected.replace('{bd}',_baseDir)
		self.assertIn(expected, sql)

	@patch('albackup.compare.os')
	def test_make_tempalte_no_diffs_dir(self,os):
		os.path=MagicMock()
		os.path.exists=MagicMock(return_value=False)
		os.path.abspath=MagicMock(return_value="the_diffs_dir")
		
		self.cmp._make_template()

		os.mkdir.assert_called_once_with("the_diffs_dir")


	@patch('albackup.compare.sh')
	def test_compare(self,sh):
		self.cmp._sql_cmdfile=MagicMock()
		self.cmp._sql_cmdfile.name="my-sql-script.sql"

		wb_console=MagicMock()
		sh.Command=MagicMock(return_value=wb_console)

		self.cmp._compare()
		sh.Command.assert_called_once_with('{}/sqlworkbench/sqlwbconsole.sh'.format(_baseDir))
		wb_console.assert_called_once_with('-script=my-sql-script.sql', _in=[])