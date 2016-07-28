'''
Created on Mar 8, 2012

@author: campenberger

The script runs all tests and produces by default XML output for the tests that 
should be junit compatible. There is also an option to produce the traditional
text output
'''
import unittest
import logging
import os
import sys
import xmlrunner 
from optparse import OptionParser

baseDir=os.path.abspath(os.path.join(os.path.dirname(__file__),'..'))

class UnitTestFailed(Exception):
    pass

def run(opts=None):
    logging.basicConfig(level=logging.CRITICAL)
    
    if not opts:
        pars = OptionParser("Usage: %prog [options]")
        pars.add_option("-x", "--xml", action="store_true", dest="useXMLRunner", default=False, help="Uses the unittest.TextTestRunner instead of an XML runner")
        pars.add_option('-v', "--verbosity", action="store", dest="verbosity", default=2, help="unittest.TextTestRunner verbosity level", type="int")
        (opts, args) = pars.parse_args()
        
    loader=unittest.TestLoader()
    suite=loader.discover(os.path.join(baseDir,"test"),'[Tt]est*.py',top_level_dir=os.path.join(baseDir,"test"))
    
    if opts.useXMLRunner:
        runner=xmlrunner.XMLTestRunner(output=os.path.join(baseDir, 'test-reports'))
    else:
        runner=unittest.TextTestRunner(verbosity=opts.verbosity)
    result=runner.run(suite)
    if not result.wasSuccessful():
        raise UnitTestFailed()
