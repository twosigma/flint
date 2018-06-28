#
#  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

''' The common code for all Flint unit tests '''
import os
import sys
import collections
from tests.ts.base_test_case import BaseTestCase


class SparkTestCase(BaseTestCase):
    ''' Base class for all Flint tests '''
    @classmethod
    def setUpClass(cls):
        ''' The automatic setup method for subclasses '''
        cls.__setup()

    @classmethod
    def tearDownClass(cls):
        ''' The automatic tear down method for subclasses '''
        cls.__teardown()

    @classmethod
    def __setup(cls, options=None):
        '''Starts spark and sets attributes `sc,sqlContext and flintContext'''
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext
        from ts.flint import FlintContext

        default_options = (SparkConf()
                           .setAppName(cls.__name__)
                           .setMaster("local"))
        setattr(cls, '_env', dict(os.environ))
        setattr(cls, '_path', list(sys.path))
        options = collections.ChainMap(options, default_options)
        spark_context = SparkContext(conf=SparkConf(options))
        sql_context = SQLContext(spark_context)
        flint_context = FlintContext(sql_context)
        setattr(cls, 'sc', spark_context)
        setattr(cls, 'sqlContext', sql_context)
        setattr(cls, 'flintContext', flint_context)

    @classmethod
    def __teardown(cls):
        '''Shuts down spark and removes attributes sc, and sqlContext'''
        cls.sc.stop()
        cls.sc._gateway.shutdown()
        cls.sc._gateway = None

        SparkContext._jvm = None
        SparkContext._gateway = None

        delattr(cls, 'sqlContext')
        delattr(cls, 'sc')
        os.environ = cls._env
        sys.path = cls._path
        delattr(cls, '_env')
        delattr(cls, '_path')
