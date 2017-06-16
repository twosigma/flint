'''
    A parent class for all Flint unit tests
'''
#  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import os
import sys
import unittest
import collections
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from ts.flint import FlintContext


_DEFAULT_OPTIONS = {}

def setup(obj, options=None):
    '''Starts spark and sets attributes ``sc``,``sqlContext`` and ``flintContext``.
    '''
    setattr(obj, '_env', dict(os.environ))
    setattr(obj, '_path', list(sys.path))
    options = collections.ChainMap(options, _DEFAULT_OPTIONS)
    spark_context = SparkContext(conf=SparkConf(options))
    sql_context = SQLContext(spark_context)
    flint_context = FlintContext(sql_context)
    setattr(obj, 'sc', spark_context)
    setattr(obj, 'sqlContext', sql_context)
    setattr(obj, 'flintContext', flint_context)

def teardown(obj):
    '''Shuts down spark and removes attributes ``sc``, and ``sqlContext``.
    '''
    obj.sc.stop()
    SparkContext._gateway.shutdown()
    SparkContext._gateway = None
    delattr(obj, 'sqlContext')
    delattr(obj, 'sc')
    os.environ = obj._env
    sys.path = obj._path
    delattr(obj, '_env')
    delattr(obj, '_path')

class SparkTestCase(unittest.TestCase):
    ''' Base class for all Flint tests
    '''
    @classmethod
    def options(cls):
        ''' Return the options '''
        return SparkConf().setAppName(cls.__name__).setMaster("local")
    @classmethod
    def setUpClass(cls):
        ''' The automatic setup method for subclasses '''
        setup(cls, options=cls.options)
    @classmethod
    def tearDownClass(cls):
        ''' The automatic tear down method for subclasses '''
        teardown(cls)
