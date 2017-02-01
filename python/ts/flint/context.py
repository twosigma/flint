#
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

'''Contains FlintContext, which helps read from Two Sigma's data
sources.
'''

import functools
import inspect
import types

import py4j
from pyspark.sql.readwriter import DataFrameReader

from . import readwriter
from . import utils


class FlintContext(object):
    '''Main entry point for time-series Spark functionality.

    A :class:`FlintContext` can be used to create a
    :class:`ts.flint.TimeSeriesDataFrame` from TS data sources.  Those
    can then be manipulated with methods from that class, and by using
    summarizers from :mod:`ts.flint.summarizers`.

    :param sqlContext: The :class:`pyspark.sql.SQLContext` backing
        this :class:`FlintContext`.

    '''

    def check_classpath(self):
        '''Verifies that the classpath available to Spark contains the flint
        scala libraries we need.

        Raises an ImportError if flint classes are missing.
        '''

        msg = ('Could not find com.twosigma.flint.timeseries.TimeSeriesRDD in '
               "the JVM's classpath, you may not have flint available")
        try:
            sc = self._sc
            cls = getattr(
                utils.jvm(sc).com.twosigma.flint.timeseries, 'TimeSeriesRDD$')
            tsrdd = getattr(cls, 'MODULE$')
            # If the class doesn't exist, py4j assumes it is a package
            # name instead, doesn't actually try to resolve it, and
            # just waits for you to try to touch a class inside that
            # package. So if it doesn't exist, it's of type
            # JavaPackage.
            if type(tsrdd) == type(py4j.java_gateway.JavaPackage):
                raise ImportError(msg)
        except Exception as e:
            raise ImportError(msg) from e

    def __init__(self, sqlContext):
        self._sqlContext = sqlContext
        self._sc = self._sqlContext._sc
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm

        self.check_classpath()

    @property
    def read(self):
        '''Entry point to access TS data.  Returns a
        :class:`.readwriter.TSDataFrameReader` which can be used to
        read data sources.
        '''

        return readwriter.TSDataFrameReader(self)
