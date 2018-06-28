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

'''This module contains summarizer routines suitable as arguments to:

- :meth:`.TimeSeriesDataFrame.summarizeCycles`
- :meth:`.TimeSeriesDataFrame.summarizeIntervals`
- :meth:`.TimeSeriesDataFrame.summarizeWindows`
- :meth:`.TimeSeriesDataFrame.summarize`
- :meth:`.TimeSeriesDataFrame.addSummaryColumns`

Example:

    >>> from ts.flint import summarizers
    >>> prices.summarize(summarizers.correlation('openPrice', 'closePrice'), key='id')

'''

from . import java
from . import utils


__all__ = [
    'SummarizerFactory',
    'correlation',
    'count',
    'covariance',
    'dot_product',
    'ema_halflife',
    'ewma',
    'geometric_mean',
    'kurtosis',
    'linear_regression',
    'max',
    'mean',
    'min',
    'nth_central_moment',
    'nth_moment',
    'product',
    'quantile',
    'skewness',
    'stddev',
    'sum',
    'variance',
    'weighted_correlation',
    'weighted_covariance',
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
        self._prefix = None

    def _jsummarizer(self, sc):
        # TODO: There should be a standard way to converting python
        #       collection to scala collections, if they are not auto
        #       converted by py4j.
        args = [utils.py_col_to_scala_col(sc, arg) for arg in self._args]
        jsummarizer = java.Packages(sc).Summarizers.__getattr__(self._func)(*args)
        if self._prefix is not None:
            jsummarizer = jsummarizer.prefix(self._prefix)
        return jsummarizer

    def __str__(self):
        return "%s(%s)" % (self._func, ", ".join(str(arg) for arg in self._args))

    def prefix(self, prefix):
        '''Adds prefix to the column names of output schema.
        All columns names will be prepended as format '<prefix>_<column>'.
        '''
        self._prefix = prefix
        return self


def rows():
    return SummarizerFactory('rows', 'rows')


def arrow(cols, include_base_rows):
    return SummarizerFactory('arrow', cols, include_base_rows)


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


def weighted_correlation(x_column, y_column, weight_column):
    '''Computes weighted correlation of two columns.

    **Adds columns:**

    <x_column>_<y_column>_<weight_column>_weightedCorrelation (*float*)
        correlation of x_column and y_column

    :param x_column: name of column X
    :type x_column: str
    :param y_column: name of column y
    :type y_column: str
    :param weight_column: name of weight column
    :type weight_column: str
    '''
    return SummarizerFactory('weightedCorrelation', x_column, y_column, weight_column)


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

    <x_column>_<y_column>_covariance (*float*)
        covariance of x_column and y_column

    :param x_column: name of column X
    :type x_column: str
    :param y_column: name of column y
    :type y_column: str
    '''
    return SummarizerFactory('covariance', x_column, y_column)


def weighted_covariance(x_column, y_column, weight_column):
    '''Computes unbiased weighted covariance of two columns.

    **Adds columns:**

    <x_column>_<y_column>_<weight_column>_weightedCovariance (*float*)
        covariance of x_column and y_column

    :param x_column: name of column X
    :type x_column: str
    :param y_column: name of column y
    :type y_column: str
    :param weight_column: name of weight column
    :type weight_column: str
    '''
    return SummarizerFactory('weightedCovariance', x_column, y_column, weight_column)


def dot_product(x_column, y_column):
    '''Computes the dot product of two columns.

    **Adds columns:**

    <x_column>_<y_column>_dotProduct (*float*)
        the dot product of x_column and y_column

    :param x_column: name of column X
    :type x_column: str
    :param y_column: name of column y
    :type y_column: str
    '''
    return SummarizerFactory('dotProduct', x_column, y_column)


def ema_halflife(column, halflife_duration, time_column='time', interpolation='previous', convention='legacy'):
    '''Calculates the exponential moving average given a specified half life. Supports the same default behaviors
    as the previous in-house implementation.

    See doc/ema.md for details on different EMA implementations.

    **Adds columns:**

    <column>_ema (*float*)
        The exponential moving average of the column

    :param column: name of the column to be summarized
    :type column: str
    :param halflife_duration: string representing duration of the half life
    :type halflife_duration: str
    :param time_column: Name of the time column.
    :type time_column: str
    :param interpolation: the interpolation type of the ema - options are 'previous', 'current', and 'linear'
    :type: interpolation: str
    :param convention: the convention used to compute the final output - options are 'convolution', 'core', and
        'legacy'
    '''
    return SummarizerFactory('emaHalfLife', column, halflife_duration, time_column, interpolation, convention)


def ewma(column, alpha=0.05, time_column='time', duration_per_period='1d', convention='legacy'):
    """Calculate the exponential weighted moving average over a column.

    It maintains a primary EMA for the series ``x_1, x_2, ...`` as well as
    an auxiliary EMA for the series ``1.0, 1.0, ...``.
    The primary EMA ``EMA_p(X)`` keeps track of the sum of the weighted series,
    whereas the auxiliary EMA ``EMA_a(X)`` keeps track of the sum of the weights.

    The weight of i-th value ``decay(t_i, t_n)`` is:
    ``decay(t_i, t_n) = exp(ln(1 - alpha) * (t_n - t_i) / duration_per_period)``

    If duration_per_period is "constant", the decay will be defined as
    ``decay(t_i, t_n) = exp(ln(1 - alpha) * (n - i))``

    Finally, if the convention is "core", we will the following EMA(X)
    as output where
    ``(EMA(X))_i = (EMA_p(X))_i / (EMA_a(X))_i``

    However, if the convention is "legacy", we will simply return EMA(X)
    such that
    ``(EMA(X))_i = (EMA_p(X))_i``

    See doc/ema.md for details on different EMA implementations.

    **Adds columns:**

    <column>_ewma (*float*)
        The exponential weighted moving average of the column

    :param column: name of the column to be summarized
    :type column: str
    :param alpha: parameter setting the decay rate of the average
    :type alpha: float
    :param time_column: Name of the time column.
    :type time_column: str
    :param duration_per_period: duration per period. The option could be
        "constant" or any string that specifies duration like "1d", "1h",
        "15m" etc. If it is "constant", it will assume that the number
        of periods between rows is constant (c = 1); otherwise, it will
        use the duration to calculate how many periods should be
        considered to have passed between any two given timestamps.
    :type duration_per_period: str
    :param convention: the convention used to compute the final output - options are 'core' and
        'legacy'.
    """
    return SummarizerFactory('ewma', column, alpha, time_column, duration_per_period, convention)


def geometric_mean(column):
    '''Computes the geometric mean of a column.

    **Adds columns:**

    <column>_geometricMean (*float*)
        The geometric mean of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('geometricMean', column)


def kurtosis(column):
    '''Computes the excess kurtosis of a column.

    **Adds columns:**

    <column>_kurtosis (*float*)
        The excess kurtosis of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('kurtosis', column)


def linear_regression(y_column, x_columns, weight_column=None, *, use_intercept=True, ignore_constants=False,
                      constant_error_bound=1.0E-12):
    '''Computes a weighted multiple linear regression of the values in
    several columns against values in another column, using values
    from yet another column as the weights.

    .. note:: Constant columns
        When there is at least one constant variable in x_columns with intercept = True or
        there are multiple constant variables in x_columns, a regression will
        fail unless ignore_constants is True.

        When ignore_constants is True, the scalar fields of regression result are the same as
        if the constant variables are not included in x_columns.
        The output beta, tStat, stdErr still have the same dimension as
        x_columns. Entries corresponding to constant variables
        will have 0.0 for beta and stdErr; and NaN for tStat.

        When ignore_constants is False and x_columns includes constant variables,
        the regression will output NaN for all regression
        result.

        If there are multiple constant variables in x_columns and the user
        wants to include a constant variable, it is recommended to set
        both of ignore_constant and use_intercept to be True.

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

    logLikelihood (*float*)
        The log-likelihood of the data given the fitted model

    akaikeIC (*float*)
        The Akaike information criterion of the data given the fitted model

    bayesIC (*float*)
        The Bayes information criterion of the data given the fitted model

    cond (*float*)
        The condition number of Gramian matrix, i.e. X^TX.

    const_columns (*list* of *string*)
        The list of variables in x_columns that are constants.

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
        intercept term. (default: True)
    :type use_intercept: bool
    :param ignore_constants: Whether the regression should ignore
        independent variables, defined by x_columns, that are constants.
        See constant columns above. (default: False)
    :type ignore_constants: bool
    :param constant_error_bound: Used when ignore_constants = True, otherwise
        ignored. The error bound on (|observations| * variance) to determine
        if a variable is constant. A variable will be considered as a constant
        c if and only if the sum of squared differences to c is less than the
        error bound. Default is 1.0E-12.
    :type constant_error_bound: float
    '''
    return SummarizerFactory('OLSRegression', y_column, x_columns, weight_column, use_intercept, ignore_constants,
                             constant_error_bound)


def max(column):
    '''Get the max of a column.

    **Adds columns:**

    <column>_max
        The max of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('max', column)


def mean(column):
    '''Computes the arithmetic mean of a column.

    **Adds columns:**

    <column>_mean (*float*)
        The mean of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('mean', column)


def min(column):
    '''Get the min of a column.

    **Adds columns:**

    <column>_min
        The min of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('min', column)

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


def product(column):
    '''Computes the product of a column.

    **Adds columns:**

    <column>_product (*float*)
        The product of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('product', column)


def quantile(column, phis):
    '''Computes the quantiles of the values in a column.

    **Adds columns:**

    <column>_<phi>quantile (*float*)
        The quantiles of values in ``<column>``.

    :param sc: Spark context
    :type sc: SparkContext
    :param column: name of the column to be summarized
    :type column: str
    :param phis: the quantiles to compute, ranging in value from (0.0,1.0]
    :type phis: list

    '''
    return SummarizerFactory('quantile', column, phis)


def skewness(column):
    '''Computes the skewness of a column.

    **Adds columns:**

    <column>_skewness (*float*)
        The skewness of the column

    :param column: name of the column to be summarized
    :type column: str
    '''
    return SummarizerFactory('skewness', column)


def stddev(column):
    '''Computes the stddev of a column

    **Adds columns:**

    <column>_stddev (*float*)
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

    **Adds columns:**

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
    return SummarizerFactory('weightedMeanTest', value_column, weight_column)


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


def compose(sc, summarizer):
    if isinstance(summarizer, SummarizerFactory):
        return summarizer
    elif isinstance(summarizer, list) or isinstance(summarizer, tuple):
        return SummarizerFactory('compose', [s._jsummarizer(sc) for s in summarizer])
    else:
        raise ValueError("summarizer should be a SummarizerFactory, a list, or a tuple")
