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

'''ts.flint contains python bindings for Flint.'''

from .context import FlintContext
from .dataframe import TimeSeriesDataFrame
from .group import TimeSeriesGroupedData
from .functions import udf

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

__author__ = 'Li Jin, Leif Walsh'
__maintainer__ = 'Li Jin, Leif Walsh'
__email__ = 'ljin@twosigma.com, leif@twosigma.com'
