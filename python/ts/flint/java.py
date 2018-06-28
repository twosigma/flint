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

import functools

from . import utils


class Packages:
    def __init__(self, sc):
        self.sc = sc

    @property
    @functools.lru_cache()
    def TimeSeriesRDD(self):
        return utils.scala_object(utils.jvm(self.sc).com.twosigma.flint.timeseries, "TimeSeriesRDD")

    @property
    @functools.lru_cache()
    def write(self):
        return utils.scala_package_object(utils.jvm(self.sc).com.twosigma.flint.timeseries.io.write)

    def new_reader(self):
        try:
            return utils.jvm(self.sc).com.twosigma.flint.timeseries.io.read.TSReadBuilder()
        except TypeError:
            return utils.jvm(self.sc).com.twosigma.flint.timeseries.io.read.ReadBuilder()

    @property
    @functools.lru_cache()
    def ArrowSummarizer(self):
        return utils.scala_object(utils.jvm(self.sc).com.twosigma.flint.timeseries.summarize.summarizer,
                                  'ArrowSummarizer')

    @property
    @functools.lru_cache()
    def ArrowWindowBatchSummarizer(self):
        return utils.scala_object(utils.jvm(self.sc).com.twosigma.flint.timeseries.window.summarizer,
                                  'ArrowWindowBatchSummarizer')

    @property
    @functools.lru_cache()
    def Summarizers(self):
        return utils.scala_object(utils.jvm(self.sc).com.twosigma.flint.timeseries, "Summarizers")

    @property
    @functools.lru_cache()
    def Windows(self):
        return utils.jvm(self.sc).com.twosigma.flint.timeseries.Windows

    @property
    @functools.lru_cache()
    def PartitionPreservingOperation(self):
        return utils.jvm(self.sc).org.apache.spark.sql.PartitionPreservingOperation

    @property
    @functools.lru_cache()
    def OrderPreservingOperation(self):
        return utils.jvm(self.sc).org.apache.spark.sql.OrderPreservingOperation
