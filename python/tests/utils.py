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
"""
    Utility functions for tests
"""

from pandas.util.testing import assert_frame_equal
import pandas as pd

def make_pdf(data, schema, dtypes=None):
    d = {schema[i]:[row[i] for row in data] for i in range(len(schema))}
    df = pd.DataFrame(data=d)[schema]

    if dtypes:
        df = df.astype(dict(zip(schema, dtypes)))

    if 'time' in df.columns:
        df = df.assign(time=pd.to_datetime(df['time'], unit='s'))

    # Hacky way of converting long to timestamps for nested DataFrame
    # Assuming (1) sub rows are presented as tuples (2) time is the first column and is long
    for col in list(df.columns):
        for i in range(len(df[col])):
            cell = df[col][i]
            if isinstance(cell, list) and len(cell) > 0 and isinstance(cell[0], tuple):
                df[col].at[i] = [((pd.to_datetime(r[0], unit='s'),) + r[1:]) for r in cell]

    return df

def make_timestamp(seconds):
    return pd.to_datetime(seconds, unit='s')

def custom_isclose(a, b, rel_prec=1e-9):
    return abs(a - b) <= rel_prec * max(abs(a), abs(b))

def _assert(a, b, cmd, check, **kwargs):
    header = "{}:\n".format(cmd) if cmd else ""
    if isinstance(a, pd.DataFrame):
        assert check(a.equals(b)), "{}Left:\n{}\nRight:\n{}".format(header, str(a), str(b))
    elif isinstance(a, float) and isinstance(b, float):
        assert check(custom_isclose(a, b)), "{}Left:\n{}\nRight:\n{}".format(header, str(a), str(b))
    else:
        assert check(a == b), "{}Left:\n{}\nRight:\n{}".format(header, str(a), str(b))

def assert_same(a, b, cmd="", **kwargs):
    _assert(a, b, cmd, lambda x: x)

def assert_not_same(a, b, cmd="", **kwargs):
    _assert(a, b, cmd, lambda x: not x)

def assert_true(a, cmd=""):
    assert a, cmd

def assert_false(a, cmd=""):
    assert not a, cmd

def get_test_base():
    from tests.pypusa_test_case import PypusaTestCase
    return PypusaTestCase
