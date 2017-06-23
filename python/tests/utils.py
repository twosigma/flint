#
#  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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
"""
    Utility functions for tests
"""

from pandas.util.testing import assert_frame_equal
import pandas as pd


def make_pdf(data, schema):
    ''' Make a Pandas DataFrame from data '''
    d = {schema[i]: [row[i] for row in data] for i in range(len(schema))}
    return pd.DataFrame(data=d)[schema]


def assert_same(object_1, object_2, criteria=None):
    """ Assert object_1 is equal to object_2 """
    if isinstance(object_1, float):
        assert criteria, object_1 == object_2
    else:
        assert_frame_equal(object_1, object_2)


def assert_true(object_1, description):
    """ Assert object_1 evaluates to True """
    assert object_1, description
