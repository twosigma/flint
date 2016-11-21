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

'''This module contains summarizer routines suitable as arguments to:

- :meth:`.TimeSeriesDataFrame.summarizeCycles`
- :meth:`.TimeSeriesDataFrame.summarizeIntervals`
- :meth:`.TimeSeriesDataFrame.summarizeWindows`
- :meth:`.TimeSeriesDataFrame.summarize`
- :meth:`.TimeSeriesDataFrame.addSummaryColumns`

Example:

    >>> from ts.flint import summarizers
    >>> prices.summarize(summarizers.correlation('openPrice', 'closePrice'), key='tid')

'''

from . import java
from . import utils
from .dataframe import TimeSeriesDataFrame


__all__ = [
    'correlation',
    'count',
    'covariance',
    'linear_regression',
    'mean',
    'nth_central_moment',
    'nth_moment',
    'stddev',
    'sum',
    'variance',
    'weighted_mean',
    'zscore',
]


class SummarizerFactory:
    '''SummarizerFactory represents an intended summarization that will be
    instantiated later when we have access to a SparkContext.

    Typical usage is that a user constructs one of these (using the
    summarization functions in this module), then passes it to one of
    the summarize methods of :class:`TimeSeriesDataFrame`, where we
    have a SparkContext.  In those methods, we have this factory
    construct the actual summarizer the user wanted.

    '''
    def __init__(self, func, *args):
        self._args = args
        self._func = func

    def _jsummarizer(self, sc):
        # TODO: There should be a standard way to converting python
        #       collection to scala collections, if they are not auto
        #       converted by py4j.
        args = [utils.py_col_to_scala_col(sc, arg) for arg in self._args]
        return java.Packages(sc).Summarizers.__getattr__(self._func)(*args)

    def __str__(self):
        return "%s(%s)" % (self._func, ", ".join(str(arg) for arg in self._args))


def correlation(cols, other=None):
    '''Computes pairwise correlation of columns.

    Example:
        >>> from ts.flint import summarizers
        >>> # Compute correlation between columns 'col1' and 'col2'
        ... prices.summarize(summarizers.correlation('col1', 'col2'))

        >>> # Compute pairwise correlations for columns 'col1', 'col2' and 'col3',
        ... prices.summarize(summarizers.correlation(['co1', 'col2', 'col3']))

        >>> # Compute only correlations for pairs of columns
        ... # ('col1', 'col3'), ('col1', 'col4'), ('col1', 'col5') and
        ... # ('col2', 'col3'), ('col2', 'col4'), ('col1', 'col5')
        ... prices.summarize(summarizers.correlation(['col1', 'col2'], ['col3', 'col4', 'col5']))

    **Adds columns:**

    <col1>_<col2>_correlation (*float*)
        The correlation between columns ``<col1>`` and ``<col2>``.

    <col1>_<col2>_correlationTStat (*float*)
        The t-stats of the correlation coefficient between the columns.

    :param cols: names of the columns to be summarized. If the ``other``
        is None, this summarizer will compute pairwise correlation and
        t-stats for only columns in ``cols``.
    :type cols: str, list of str
    :param other: other names of columns to be summarized. If it is None,
        this summarizer will compute pairwise correlation and t-stats
        for columns only in ``cols``; otherwise, it will compute all possible
        pairs of columns where the left is one of columns in ``cols``
        and the right is one of columns in ``other``. By default, it
        is None.
    :type other: str, list of str, None
    '''
    if isinstance(cols, str):
        cols = [cols]
    if isinstance(other, str):
        other = [other]
    if other is None:
        return SummarizerFactory('correlation', cols)
    else:
        return SummarizerFactory('correlation', cols, other)


def count():
    '''Counts the number of rows.

    **Adds columns:**

    count (*int*)
        The number of rows.

    '''
    return SummarizerFactory('count')


def covariance(x_column, y_column):
    '''Computes covariance of two columns.

    **Adds columns:**

    <column_x>_<column_y>_covariance (*float*)
        covariance of column_x and column_y

    :param column_x: name of column X
    :type column_x: str
    :param column_y: name of column y
    :type column_y: str
    '''
    return SummarizerFactory('covariance', x_column, y_column)


def linear_regression(y_column, x_columns, weight_column=None, *, use_intercept=True):
    '''Computes a weighted multiple linear regression of the values in
    several columns against values in another column, using values
    from yet another column as the weights.

    **Adds columns:**

    samples (*int*)
        The number of samples.

    r (*float*)
        Pearson's correlation coefficient.

    rSquared (*float*)
        Coefficient of determination.

    beta (*list* of *float*)
        The betas of each of ``x_columns``, without the intercept
        component.

    stdErr_beta (*list* of *float*)
        The standard errors of the betas.

    tStat_beta (*list* of *float*)
        The t-stats of the betas.

    hasIntercept (*bool*)
        True if using an intercept.

    intercept (*float*)
        The intercept fit by the regression.

    stdErr_intercept (*float*)
        The standard error of the intercept.

    tStat_intercept (*float*)
        The t-stat of the intercept.

    :param y_column: Name of the column containing the dependent
        variable.
    :type y_column: str
    :param x_columns: Names of the columns containing the independent
        variables.
    :type x_columns: list of str
    :param weight_column: Name of the column containing weights for
        the observations.
    :type weight_column: str
    :param use_intercept: Whether the regression should consider an
        intercept term.
    :type use_intercept: bool
    '''
    return SummarizerFactory('OLSRegression', y_column, x_columns, weight_column, use_intercept)


def mean(column):
    '''Computes the arithmetic mean of a column.

    **Adds columns:**

    <column>_mean (*float*)
        The mean of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('mean', column)


def nth_central_moment(column, n):
    '''Computes the nth central moment of the values in a column.

    **Adds columns:**

    <column>_<n>thCentralMoment (*float*)
        The ``n`` th central moment of values in ``<column>``.

    :param column: name of the column to be summarized
    :type column: str
    :param n: which moment to compute
    :type n: int

    '''
    return SummarizerFactory('nthCentralMoment', column, n)


def nth_moment(column, n):
    '''Computes the nth raw moment of the values in a column.

    **Adds columns:**

    <column>_<n>thMoment (*float*)
        The ``n`` th raw moment of values in ``<column>``.

    :param column: name of the column to be summarized
    :type column: str
    :param n: which moment to compute
    :type n: int

    '''
    return SummarizerFactory('nthMoment', column, n)


def stddev(column):
    '''Computes the stddev of a column

    **Adds columns:**

    <column>_sum (*float*)
        The standard deviation of ``<column>``

    :param column: name of the column to be summarized
    :type column: str

    '''
    return SummarizerFactory('stddev', column)


def sum(column):
    '''Computes the sum of the values in a column.

    **Adds columns:**

    <column>_sum (*float*)
        The sum of values in ``<column>``.

    :param column: name of the column to be summarized
    :type column: str

    '''
    return SummarizerFactory('sum', column)


def variance(column):
    '''Computes the variance of a column.

    **Addes columns:**

    <column>_variance (*float*)
        The variance of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('variance', column)


def weighted_mean(value_column, weight_column):
    '''Computes the mean, standard deviation, and t-stat of values in one
    column, with observations weighted by the values in another
    column. Also computes the number of observations.

    **Adds columns:**

    <value_column>_<weight_column>_weightedMean (*float*)
        The weighted mean of the values in ``<value_column>`` weighted
        by values in ``<weight_column>``.

    <value_column>_<weight_column>_weightedStandardDeviation (*float*)
        The weighted standard deviation of the values in
        ``<value_column>`` weighted by values in ``<weight_column>``.

    <value_column>_<weight_column>_weightedTStat (*float*)
        The t-stats of the values in ``<value_column>`` weighted by
        values in ``<weight_column>``.

    <value_column>_<weight_column>_observationCount (*int*)
        The number of observations.

    :param value_column: name of the column of values to be
        summarized
    :type value_column: str
    :param weight_column: name of the column of weights to be used
    :type weight_column: str

    '''
    return SummarizerFactory('weightedMean', value_column, weight_column)


def zscore(column, in_sample):
    '''Computes the z-score of values in a column with respect to the
    sample mean and standard deviation observed so far.

    Optionally includes the current observation in the calculation of
    the sample mean and standard deviation, if ``in_sample`` is true.

    **Adds columns:**

    <column>_zScore (*float*)
        The z-scores of values in ``<column>``.

    :param column: name of the column to be summarized
    :type column: str
    :param in_sample: whether to include or exclude a data point from
        the sample mean and standard deviation when computing the
        z-score for that data point
    :type in_sample: bool

    '''
    return SummarizerFactory('zScore', column, in_sample)
