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
'''
    The base class code for all Flint unit tests
'''
import unittest
from abc import ABCMeta, abstractclassmethod
import tests.utils as test_utils
from tests.test_data import (FORECAST_DATA, PRICE_DATA, PRICE2_DATA, VOL_DATA, VOL2_DATA,
                             VOL3_DATA, INTERVALS_DATA)
from functools import lru_cache


class BaseTestCase(unittest.TestCase, metaclass=ABCMeta):
    ''' Abstract base class for all Flint tests
    '''
    @abstractclassmethod
    def setUpClass(cls):
        ''' The automatic setup method for subclasses '''
        return

    @abstractclassmethod
    def tearDownClass(cls):
        ''' The automatic tear down method for subclasses '''
        return

    @lru_cache(maxsize=None)
    def forecast(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))

    @lru_cache(maxsize=None)
    def vol(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(VOL_DATA, ["time", "id", "volume"]))

    @lru_cache(maxsize=None)
    def vol2(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(VOL2_DATA, ["time", "id", "volume"]))

    @lru_cache(maxsize=None)
    def vol3(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(VOL3_DATA, ["time", "id", "volume"]))

    @lru_cache(maxsize=None)
    def price(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(PRICE_DATA, ["time", "id", "price"]))

    @lru_cache(maxsize=None)
    def price2(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(PRICE2_DATA, ["time", "id", "price"]))

    @lru_cache(maxsize=None)
    def intervals(self):
        return self.flintContext.read.pandas(
            test_utils.make_pdf(INTERVALS_DATA, ['time']))
