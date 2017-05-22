import sys
import unittest
import findspark
findspark.init()
sys.path.append('.')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from ts.flint import FlintContext

class PySparkTestCase(unittest.TestCase):
    """
    A base class used by all tests to share the
    various contexts
    """
    sc_values = {}

    @classmethod
    def setUpClass(cls):
        config = SparkConf().setAppName(cls.__name__).setMaster("local")
        cls.sparkContext = SparkContext(conf=config)
        cls.sqlContext = SQLContext(cls.sparkContext)
        cls.flintContext = FlintContext(cls.sqlContext)
        cls.sc_values[cls.__name__] = cls.sparkContext

    @classmethod
    def tearDownClass(cls):
        cls.sc_values.clear()
        cls.sparkContext.stop()
