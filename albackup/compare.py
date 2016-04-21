from __future__ import print_function
import logging
import json
import os
from jinja2 import Environment,FileSystemLoader
from tempfile import NamedTemporaryFile
import sh

from albackup import loggerFactory


_getLogger=loggerFactory('compare')
jina_env=Environment(loader=FileSystemLoader('templates'))

class DbCompare(object):

	TEMPLATE="compare.sql"

	def __init__(self,ref_cfg,target_cfg,sqlwb_dir):
		self.ref_cfg=ref_cfg
		self.target_cfg=target_cfg
		self.sqlwb_dir=os.path.abspath(sqlwb_dir)
		self.logger=_getLogger('DbCompare')

	def _make_template(self):
		template=jina_env.get_template(self.TEMPLATE)
		self._sql_cmdfile=NamedTemporaryFile(mode="w+")
		context={
			'ref': self.ref_cfg,
			'target': self.target_cfg,
			'sqlwb_dir': self.sqlwb_dir,
			'cwd': os.path.abspath(os.getcwd())
		}
		print(template.render(context),file=self._sql_cmdfile)
		self.logger.info('Compare command file rendered to %s',self._sql_cmdfile.name)

		self._sql_cmdfile.seek(0)
		self.logger.debug('Compare script:\n%s',self._sql_cmdfile.read())

		return self._sql_cmdfile

	def _compare(self):
		self.logger.info('Comparing database schemas... (takes a while)')

		sqlwbconsole=sh.Command(os.path.join(self.sqlwb_dir,'sqlwbconsole.sh'))
		output=str(sqlwbconsole("-script={}".format(self._sql_cmdfile.name),_in=[]))

		self.logger.debug('database compare scripted returned:\n%s',output)
		self.logger.info('Results are in diff-%s.xml and diff-%s.html',self.ref_cfg['name'],self.ref_cfg['name'])

	def run(self):
		self._make_template()
		self._compare()


if __name__ == '__main__':

	logging.basicConfig(
		level=logging.DEBUG,
		format="%(asctime)s:%(name)-40s:%(levelname)-7s:%(message)s"
	)
	logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
	logging.getLogger('sh').setLevel(logging.ERROR)
	logger=_getLogger()

	cfg=None
	with open('test.json') as fh:
		cfg=json.load(fh)
		logger.info('Read configuration from test.json')

	ref=filter(lambda db: db['name']=='astar',cfg['databases'])[0]
	logger.debug('Found ref cfg for astar: {}'.format(ref))

	comp=DbCompare(ref,cfg['restore'],'../sqlworkbench')
	comp.run()
