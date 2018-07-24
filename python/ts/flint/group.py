#
#  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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
import pyspark

__all__ = ["TimeSeriesGroupedData"]

def wrap_gd_method(method):
    @functools.wraps(method)
    def _new_method(self, *args, **kwargs):
        name = method.__name__
        actual_method = getattr(pyspark.sql.GroupedData, name)
        return actual_method(self, *args, **kwargs)
    return _new_method

class TimeSeriesGroupedData(pyspark.sql.GroupedData):
    ''' A sub class of :class:`pyspark.sql.GroupedData`.

        Currently this class is same as :class:`pyspark.sql.GroupedData` except that
        we add instrumentation on this class.
    '''
    def __init__(self, gd):
        # This doesn't work with open source version of Spark
        # because grouping_cols is ts specific
        # Fix this once we moved to Spark 2.3
        if hasattr(gd, 'grouping_cols'):
            super().__init__(gd._jgd, gd.sql_ctx, gd.grouping_cols)
        else:
            # 2.3
            super().__init__(gd._jgd, gd._df)

    @wrap_gd_method
    def agg(self, *exprs):
        pass

    @wrap_gd_method
    def count(self):
        pass

    @wrap_gd_method
    def mean(self, *cols):
        pass

    @wrap_gd_method
    def avg(self, *cols):
        pass

    @wrap_gd_method
    def max(self, *cols):
        pass

    @wrap_gd_method
    def min(self, *cols):
        pass

    @wrap_gd_method
    def sum(self, *cols):
        pass

    @wrap_gd_method
    def pivot(self, pivot_col, values=None):
        pass

    @wrap_gd_method
    def apply(self, udf):
        pass
