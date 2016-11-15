/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.summarize.SummarizerFactory
import com.twosigma.flint.timeseries.summarize.summarizer._
import org.apache.spark.sql.types._

object Summarizers {
  private[timeseries] def rows(column: String): SummarizerFactory = RowsSummarizerFactory(column)

  /**
   * Counts the number of rows.
   *
   * The output schema is:
   *   - "count": [[LongType]], the number of rows.
   *
   * @return a [[SummarizerFactory]] which could provide a summarizer to count how many rows.
   */
  def count(): SummarizerFactory = CountSummarizerFactory()

  /**
   * Calculates the summation for a given column.
   *
   * The output schema is:
   *   - "<column>_sum": [[DoubleType]], the summation of rows.
   *
   * @param column The column expected to perform the summation.
   * @return a [[SummarizerFactory]] which could provide a summarizer to sum all rows for a given column.
   */
  def sum(column: String): SummarizerFactory = SumSummarizerFactory(column)

  /**
   * Calculates the weighted mean, weighted deviation, weighted t-stat, and the count of observations.
   *
   * The output schema is:
   *   - "<valueColumn>_<weightColumn>_weightedMean": [[DoubleType]], the weighted mean of the `valueColumn`
   *     using the weights from the `weightColumn`.
   *   - "<valueColumn>_<weightColumn>_weightedStandardDeviation": [[DoubleType]], the weighted standard
   *     deviation of the `valueColumn` using the weights from the `weightColumn`.
   *   - "<valueColumn>_<weightColumn>_weightedTStat": [[DoubleType]], the weighted t-stats of the
   *     `valueColumn` using the weights from the `weightColumn`.
   *   - "<valueColumn>_<weightColumn>_observationCount": [[LongType]], the number of observations.
   *
   * @param valueColumn  The column expected to calculate the mean, deviation, t-stats etc.
   * @param weightColumn The column expected to provide a weight for each row.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the weighted mean,
   *         weighted deviation, weighted t-stat, and the count of observations.
   */
  def weightedMeanTest(valueColumn: String, weightColumn: String): SummarizerFactory =
    WeightedMeanTestSummarizerFactory(valueColumn, weightColumn)

  /**
   * Calculates the arithmetic mean for a column.
   *
   * The output schema is:
   *   - "<column>_mean": [[DoubleType]], the arithmetic mean of the `valueColumn`.
   *
   * @param column The column expected to calculate the mean.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the arithmetic mean.
   */
  def mean(column: String): SummarizerFactory = MeanSummarizerFactory(column)

  /**
   * Calculates the standard deviation for a column. This applies Bessel's correction.
   *
   * The output schema is:
   *  - "<column>_stddev": [[DoubleType]], the standard deviation of the `column`.
   *
   * @param column The column expected to calculate the standard deviation
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the standard deviation.
   */
  def stddev(column: String): SummarizerFactory = StandardDeviationSummarizerFactory(column)

  /**
   * Calculates the variance for a column. This applies Bessel's correction.
   *
   * The output schema is:
   *  - "<column>_variance": [[DoubleType]], the variance of the `column`.
   *
   * @param column The column expected to calculate the variance
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the variance.
   */
  def variance(column: String): SummarizerFactory = VarianceSummarizerFactory(column)

  /**
   * Calculates the covariance between two columns
   *
   * The output schema is:
   *  - "<columnX>_<columnY>_covariance": [[DoubleType]], the covariance of `columnX` and `columnY`
   *
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the covariance.
   */
  def covariance(columnX: String, columnY: String): SummarizerFactory = CovarianceSummarizerFactory(columnX, columnY)

  /**
   * Computes the z-score with the option for out-of-sample calculation.
   *
   * The output schema is:
   *   - "<column>_zScore": [[DoubleType]], the z-scores of specific column.
   *
   * @param column                    The column expected to calculate the z-score.
   * @param excludeCurrentObservation Whether to use unbiased sample standard deviation with current observation or
   *                                  unbiased sample standard deviation excluding current observation.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate z-score.
   */
  def zScore(column: String, excludeCurrentObservation: Boolean): SummarizerFactory =
    ZScoreSummarizerFactory(column, excludeCurrentObservation)

  /**
   * Compute the n-th moment.
   *
   * The output schema is:
   *   - "<column>_<n>thMoment": [[DoubleType]], the n-th moment of specific column.
   *
   * @param column The column expected to calculate the n-th moment.
   * @param n      The order of moment expected to calculate.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate n-th moment.
   */
  def nthMoment(column: String, n: Int): SummarizerFactory = NthMomentSummarizerFactory(column, n)

  /**
   * Compute the n-th central moment.
   *
   * The output schema is:
   *   - "<column>_<n>thCentralMoment": [[DoubleType]], the n-th central moment of specific column.
   *
   * @param column The column expected to calculate the n-th central moment.
   * @param n      The order of moment expected to calculate.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate n-th central moment.
   */
  def nthCentralMoment(column: String, n: Int): SummarizerFactory = NthCentralMomentSummarizerFactory(column, n)

  /**
   * Compute correlations for all possible pairs in `columns`.
   *
   * The output schema is:
   *   - "<column1>_<column2>_correlation": [[DoubleType]], the correlation of column "column1" and column "columns2".
   *   - "<column1>_<column2>_correlationTStat": [[DoubleType]], the t-stats of correlation between column "column1" and
   *     column "column2".
   *
   * @param columns columns expected to calculate pairwise correlations.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate correlation
   *         between all different columns.
   */
  def correlation(columns: String*): SummarizerFactory = MultiCorrelationSummarizerFactory(columns.toArray, None)

  /**
   * Compute correlations between all possible pairs of columns where the left is one of `columns` and the right is
   * one of `others`.
   *
   * The output schema is:
   *   - "<column1>_<column2>_correlation": [[DoubleType]], the correlation of column "column1" and column "column2"
   *     where "column1" is one of columns in `xColumns` and "column2" is one of columns in `yColumns`.
   *   - "<column1>_<column2>_correlationTStat": [[DoubleType]], the t-stats of correlation between column "column1" and
   *     column "column2" where "column1" is one of columns in `xColumns` and "column2" is one of columns in `yColumns`.
   *
   * @param xColumns A sequence of column names.
   * @param yColumns A sequence of column names which are expected to have distinct names to `xColumns`.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate correlation
   *         between all different columns.
   */
  def correlation(xColumns: Seq[String], yColumns: Seq[String]): SummarizerFactory =
    MultiCorrelationSummarizerFactory(xColumns.toArray, Some(yColumns.toArray))

  /**
   * Performs a weighted multiple OLS linear regression of the values in several columns against values
   * in another column, using values from yet another column as the weights.
   *
   * The output schema is:
   *   - "samples": [[LongType]], the number of samples.
   *   - "beta": [[ArrayType]], the beta without the intercept component.
   *   - "intercept": [[DoubleType]], the intercept.
   *   - "hasIntercept": [[BooleanType]], if it has intercept.
   *   - "stdErr_intercept": [[DoubleType]], the standard error of intercept.
   *   - "stdErr_beta": [[ArrayType]], the standard error of beta.
   *   - "rSquared": [[DoubleType]], the r-squared statistics.
   *   - "r": [[DoubleType]], the squared root of r-squared statistics.
   *   - "tStat_intercept": [[DoubleType]], the t-stats of intercept.
   *   - "tStat_beta": [[ArrayType]], the t-stats of beta.
   *
   * @param yColumn         Name of column containing the dependent variable.
   * @param xColumns        List of column names containing the independent variables.
   * @param weightColumn    Name of column containing weights.
   * @param shouldIntercept Whether the regression should consider an intercept term. Default is true.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate beta, intercept, stdErr,
   *         t-stats, and r etc.
   */
  def OLSRegression(
    yColumn: String,
    xColumns: Seq[String],
    weightColumn: String = null,
    shouldIntercept: Boolean = true
  ): SummarizerFactory =
    OLSRegressionSummarizerFactory(
      yColumn,
      xColumns.toArray,
      weightColumn,
      shouldIntercept
    )

  /**
   * Return a list of quantiles for a given list of quantile probabilities.
   *
   * @note The implementation of this summarizer is not quite in a streaming and parallel fashion as there
   *       is no way to compute exact quantile using one-pass streaming algorithm. When this summarizer is
   *       used in summarize() API, it will collect all the data under the `column` to the driver and thus may not
   *       be that efficient in the sense of IO and memory intensive. However, it should be fine to use
   *       in the other summarize API(s) like summarizeWindows(), summarizeIntervals(), summarizeCycles() etc.
   * @param p The list of quantile probabilities. The probabilities must be great than 0.0 and less than or equal
   *          to 1.0.
   * @return a [[SummarizerFactory]] which could provide a summarizer to compute the quantiles.
   */
  def quantile(column: String, p: Seq[Double]): SummarizerFactory = QuantileSummarizerFactory(column, p.toArray)

  /**
   * Return a summarizer that is composed of multiple summarizers.
   *
   * The output rows of the composite summarizer is a concatenation of the output rows of input summarizers
   * (time column and key columns will appear only once in the output rows).
   *
   * Column names conflict:
   * Sometimes, the output column names of multiple summarizers can be the same, for instance, "count", "beta", and etc.
   * To deal with this, user need to use [[SummarizerFactory.prefix()]] to rename conflicting output columns.\
   *
   * For instance:
   * {{{
   * compose(Summarizers.mean("column"), Summarizers.mean("column").prefix("prefix"))
   * }}}
   *
   * @param summarizers Summarizers to be composed
   * @return a [[SummarizerFactory]] which is a composition of multiple [[SummarizerFactory]](s)
   *
   */
  def compose(summarizers: SummarizerFactory*): SummarizerFactory = summarizers.reduce(CompositeSummarizerFactory(_, _))

  /**
   * Performs single exponential smoothing over a column. Primes the EMA by maintaining two EMAs, one over the series
   * (0.0, x_1, x_2, ...) and one over the series (0.0, 1.0, 1.0, ...). The smoothed series is the result of dividing
   * each element in the EMA of the first series by the element at the same index in the second series.
   *
   * More concretely, the primary EMA is caclulated as follows: suppose we have a time series
   * X = ((x_1, t_1), (x_2, t_2), ..., (x_n, t_n)). Then, we calculate the primary EMA as
   * <pre><code>(EMA<sub>p</sub> (X))<sub>i</sub> = &alpha;(t<sub>i-1</sub>, t<sub>i</sub>) (EMA<sub>p</sub> (X))<sub>i-1</sub> + (1 - &alpha;(t<sub>i-1</sub>, t<sub>i</sub>)) x<sub>i</sub> </code>
   * </pre>
   * with the initial conditions
   * <pre><code>(EMA<sub>p</sub> (X))<sub>0</sub> = 0.0, t<sub>0</sub> = t<sub>1</sub> - primingPeriods </code>
   * </pre>
   * and where <pre><code>&alpha;(t<sub>i-1</sub>, t<sub>i</sub>)</code></pre>
   * is the decay between the timestamps jointly specified by timestampsToPeriods and decayPerPeriod, i.e.
   * <pre><code>&alpha;(t<sub>i-1</sub>, t<sub>i</sub>) = exp(timestampsToPeriods(t<sub>i-1</sub>, t<sub>i</sub>) * ln(1 - decayPerPeriod)) </code>
   * </pre>
   *
   * Likewise, the auxiliary EMA is calculated as
   * <pre><code>(EMA<sub>a</sub> (X))<sub>i</sub> = &alpha;(t<sub>i-1</sub>, t<sub>i</sub>) (EMA<sub>a</sub> (X))<sub>i-1</sub> + (1 - &alpha;(t<sub>i-1</sub>, t<sub>i</sub>)) </code>
   * </pre>
   * with the same initial conditions.
   *
   * Finally, we take
   * <pre><code>(EMA (X))<sub>i</sub> = (EMA<sub>p</sub> (X))<sub>i</sub> / (EMA<sub>a</sub> (X))<sub>i</sub> </code>
   * </pre>
   *
   * @note The implementation does not find the exponential moving average via a one-pass streaming algorithm. Rather,
   *       it calculates the EMA of the data in each partition and then during the merge phase calculates correction
   *       factors to account for using the wrong decay values (primingPeriods vs. the number of periods elapsed between
   *       the last data point of the previous partition and the first data point in the current partition). Moreover,
   *       the implementation collects summary data for each data point in xColumn onto the master when used via the
   *       summarize() API which may be a concern for IO or memory-bound applications.
   * @param xColumn              Name of column containing series to be smoothed
   * @param timeColumn           Name of column containing the timestamp
   * @param decayPerPeriod       Parameter setting the decay rate of the average
   * @param primingPeriods       Parameter used to find the initial decay parameter - taken to be the time elapsed
   *                             before the first data point
   * @param timestampsToPeriods  Function that takes two longs and returns the number of periods that should be
   *                             considered to have elapsed between them
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate the exponentially smoothed
   *         series
   */
  def exponentialSmoothing(
    xColumn: String,
    timeColumn: String = TimeSeriesRDD.timeColumnName,
    decayPerPeriod: Double = 0.05,
    primingPeriods: Double = 1.0,
    timestampsToPeriods: (Long, Long) => Double = (t1: Long, t2: Long) =>
      (t2 - t1) / (24 * 60 * 60 * 1e9)
  ): SummarizerFactory =
    ExponentialSmoothingSummarizerFactory(
      xColumn,
      timeColumn,
      decayPerPeriod,
      primingPeriods,
      timestampsToPeriods
    )

  /**
   * Calculates the min for a column.
   *
   * The output schema is:
   *   - "<column>_min": Type of input column
   *
   * @param column The column expected to calculate the min.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the min.
   */
  def min(column: String): SummarizerFactory = ExtremeSummarizerFactory(column, ExtremeSummarizerType.Min)

  /**
   * Calculates the max for a column.
   *
   * The output schema is:
   *   - "<column>_max": Type of input column
   *
   * @param column The column expected to calculate the max.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the max.
   */
  def max(column: String): SummarizerFactory = ExtremeSummarizerFactory(column, ExtremeSummarizerType.Max)

  // TODO: These might be useful to implement

  // def geometricMean

  // def describe

  // def product

  // def skewness

  // def kurtosis
}
