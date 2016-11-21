#
#  Copyright 2015 TWO SIGMA OPEN SOURCE, LLC
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

import pandas as pd
import py4j
import time, calendar, datetime

# Floating precision checks:
#   - Relative difference of 1e-9
#   - Pandas precision up to 6 digits of precision

__all__ = ['assert_same', 'assert_not_same',
           'assert_true', 'assert_false']

def custom_isclose(a, b, rel_prec=1e-9):
    return abs(a - b) <= rel_prec * max(abs(a), abs(b))

def _assert(a, b, cmd, check, **kwargs):
    header = "{}:\n".format(cmd) if cmd else ""
    if isinstance(a, pd.DataFrame):
        a_sorted = a.sort_index(axis=1)
        b_sorted = b.sort_index(axis=1)
        assert check(a_sorted.equals(b_sorted)), "{}Left:\n{}\nRight:\n{}".format(header, str(a_sorted), str(b_sorted))
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

def assert_java_object_exists(jobj, cmd):
    assert_not_same(type(jobj), type(py4j.java_gateway.JavaPackage), cmd)

def to_nanos(when):
    supported_format = ["%Y%m%d %H:%M:%S", "%Y%m%d"]
    t = None
    for f in supported_format:
        try:
            t = time.strptime(when, f)
        except ValueError:
            pass

    if t is None:
        raise ValueError("{0} is not in supported format {1}".format(when, supported_format))

    return calendar.timegm(t) * 1000 * 1000 * 1000
