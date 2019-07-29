import unittest
from test_search import TestSearch
from test_topic_modelling import *
from test_report import TestReport
from test_utilities import *
import sys

def make_suite(TestClass):
    suite = unittest.TestSuite()
    suite.addTest(TestClass('test_query'))
    suite.addTest(TestClass('test_task_result'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    if len(sys.argv) > 1:
        tests = [globals()[t] for t in sys.argv[1:]]
       
    else:
        tests = [ TestUtilityList,
                       TestSearch,
    		       TestExtractDocID,
    		       TestExtractWords,
    		       TestFindSteps,
    		       TestExtractFacets,
    		       TestGenerateTimeseries,
    		       TestCommonFacetValues,
    		       TestReport,
                       TestTopic,
    		       TestTopicLinking
                           ]
    errors, failures, skipped, total = 0,0,0,0
    for TestClass in tests:
        print("")
        print(TestClass.__name__)
        result = runner.run(make_suite(TestClass))
        errors   += len(result.errors  )
        failures += len(result.failures)
        skipped  += len(result.skipped )
        total    += result.testsRun
    print("Total: %d, errors: %d, failures: %d, skipped: %d" %(total, errors, failures, skipped))

