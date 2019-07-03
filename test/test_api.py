import unittest
from test_search import TestSearch
from test_topic_modelling import TestTopic
from test_report import TestReport
from test_utilities import *

def make_suite(TestClass):
    suite = unittest.TestSuite()
    suite.addTest(TestClass('test_query'))
    suite.addTest(TestClass('test_task_result'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()

    for TestClass in [TestSearch,
                      TestTopic,
                      TestUtilityList,
                      TestExtractDocID,
                      TestExtractWords,
                      TestFindSteps,
                      TestExtractFacets,
                      TestGenerateTimeseries,
                      TestCommonFacetValues,
                      TestReport]:
        print("")
        print(TestClass.__name__)
        runner.run(make_suite(TestClass))


