/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.annotation.PythonApi
import com.twosigma.flint.timeseries.summarize.{
  OverlappableSummarizerFactory,
  SummarizerFactory
}
import com.twosigma.flint.timeseries.summarize.summarizer._
import org.apache.spark.sql.types._

import scala.concurrent.duration.Duration

object Summarizers {

  @PythonApi
  private[timeseries] def rows(column: String): SummarizerFactory =
    RowsSummarizerFactory(column)

  @PythonApi(until = "0.4.0")
  @deprecated("Use arrow summarizer with includeBaseRows")
  private[timeseries] def arrow(columns: Seq[String]): SummarizerFactory =
    ArrowSummarizerFactory(columns, false)

  @PythonApi
  private[timeseries] def arrow(columns: Seq[String], includeBaseRows: Boolean): SummarizerFactory =
    ArrowSummarizerFactory(columns, includeBaseRows)
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
   * Counts non null values in a column.
   *
   * The output schema is:
   *   - "<column>_count": [[LongType]], the number of non null values.
   *
   * @return a [[SummarizerFactory]]
   */
  def count(column: String): SummarizerFactory = CountSummarizerFactory(column)

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
  def weightedMeanTest(
    valueColumn: String,
    weightColumn: String
  ): SummarizerFactory =
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
  def stddev(column: String): SummarizerFactory =
    StandardDeviationSummarizerFactory(column)

  /**
   * Calculates the variance for a column. This applies Bessel's correction.
   *
   * The output schema is:
   *  - "<column>_variance": [[DoubleType]], the variance of the `column`.
   *
   * @param column The column expected to calculate the variance
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the variance.
   */
  def variance(column: String): SummarizerFactory =
    VarianceSummarizerFactory(column)

  /**
   * Calculates the covariance between two columns
   *
   * The output schema is:
   *  - "<xColumn>_<yColumn>_covariance": [[DoubleType]], the covariance of `xColumn` and `yColumn`
   *
   * @param xColumn A column to compute covariance
   * @param yColumn The other column to compute covariance
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the covariance.
   */
  def covariance(xColumn: String, yColumn: String): SummarizerFactory =
    CovarianceSummarizerFactory(xColumn, yColumn)

  /**
   * Calculates the unbiased weighted covariance between two columns
   *
   * The output schema is:
   *  - "<xColumn>_<yColumn>_<weightColumn>_weightedCovariance": [[DoubleType]], the weighted covariance of
   *    `xColumn` and `yColumn` with weight `weightColumn`.
   *
   * @param xColumn      A column to compute covariance
   * @param yColumn      The other column to compute covariance
   * @param weightColumn The column whose values will be served as weights
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the covariance.
   */
  def weightedCovariance(
    xColumn: String,
    yColumn: String,
    weightColumn: String
  ): SummarizerFactory =
    WeightedCovarianceSummarizerFactory(
      xColumn,
      yColumn,
      weightColumn
    )

  /**
   * Computes the z-score with the option for out-of-sample calculation.
   *
   * The output schema is:
   *   - "<column>_zScore": [[DoubleType]], the z-scores of specific column.
   *
   * @param column                    The column expected to calculate the z-score.
   * @param includeCurrentObservation Whether to use unbiased sample standard deviation with current observation or
   *                                  unbiased sample standard deviation excluding current observation.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate z-score.
   */
  def zScore(
    column: String,
    includeCurrentObservation: Boolean
  ): SummarizerFactory =
    ZScoreSummarizerFactory(column, includeCurrentObservation)

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
  def nthMoment(column: String, n: Int): SummarizerFactory =
    NthMomentSummarizerFactory(column, n)

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
  def nthCentralMoment(column: String, n: Int): SummarizerFactory =
    NthCentralMomentSummarizerFactory(column, n)

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
  def correlation(columns: String*): SummarizerFactory =
    columns
      .combinations(2)
      .map {
        case Seq(colX, colY) =>
          CorrelationSummarizerFactory(colX, colY)
            .asInstanceOf[SummarizerFactory]
      }
      .reduce(Summarizers.compose(_, _))

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
  def correlation(
    xColumns: Seq[String],
    yColumns: Seq[String]
  ): SummarizerFactory = {
    val duplicateColumns = xColumns.intersect(yColumns)
    require(
      duplicateColumns.isEmpty,
      s"Found duplicate input columns: $duplicateColumns"
    )
    (for (xColumn <- xColumns; yColumn <- yColumns)
      yield CorrelationSummarizerFactory(xColumn, yColumn)
      .asInstanceOf[SummarizerFactory])
      .reduce(Summarizers.compose(_, _))
  }

  /**
   * Calculates the Pearson weighted correlation between two columns
   *
   * The output schema is:
   *  - "<xColumn>_<yColumn>_<weightColumn>_weightedCorrelation": [[DoubleType]], the weighted correlation of
   *    `xColumn` and `yColumn` with weight `weightColumn`.
   *
   * @param xColumn      A column to compute correlation
   * @param yColumn      The other column to compute correlation
   * @param weightColumn The column whose values will be served as weights
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the correlation.
   */
  def weightedCorrelation(
    xColumn: String,
    yColumn: String,
    weightColumn: String
  ): SummarizerFactory =
    WeightedCorrelationSummarizerFactory(xColumn, yColumn, weightColumn)

  /**
   * Performs a weighted multiple OLS linear regression of the values in several columns against values
   * in another column, using values from yet another column as the weights.
   *
   * The output schema is:
   *   - "samples": [[LongType]], the number of samples.
   *   - "beta": [[ArrayType]] of [[DoubleType]], the beta without the intercept component.
   *   - "intercept": [[DoubleType]], the intercept.
   *   - "hasIntercept": [[BooleanType]], if it has intercept.
   *   - "stdErr_intercept": [[DoubleType]], the standard error of intercept.
   *   - "stdErr_beta": [[ArrayType]] of [[DoubleType]], the standard error of beta.
   *   - "rSquared": [[DoubleType]], the r-squared statistics.
   *   - "r": [[DoubleType]], the squared root of r-squared statistics.
   *   - "tStat_intercept": [[DoubleType]], the t-stats of intercept.
   *   - "tStat_beta": [[ArrayType]] of [[DoubleType]], the t-stats of beta.
   *   - "logLikelihood": [[DoubleType]], the log-likelihood of the data given the fitted betas.
   *   - "akaikeIC": [[DoubleType]], the Akaike information criterion.
   *   - "bayesIC": [[DoubleType]], the Bayes information criterion.
   *   - "cond": [[DoubleType]], the condition number Gramian matrix, i.e. X^TX.
   *   - "const_columns": [[ArrayType]] of [[StringType]], the list of variables in `xColumns` that are constants.
   *
   *
   * @param yColumn               Name of column containing the dependent variable.
   * @param xColumns              List of column names containing the independent variables.
   * @param weightColumn          Name of column containing weights.
   * @param shouldIntercept       Whether the regression should consider an intercept term. Default is true.
   * @param shouldIgnoreConstants Whether the regression should ignore independent variables, defined by `xColumns`,
   *                              that are constants. When it is true, the scalar fields
   *                              of regression result are the same as if the constant variables are not
   *                              included in `xColumns`. The output beta, tStat, stdErr still have the same
   *                              dimension as `xColumns`. However, entries corresponding to constant variables
   *                              will have 0.0 for beta and stdErr; and Double.NaN for tStat.
   *                              When it is false and if `xColumns` includes constant variables, the regression
   *                              will output Double.NaN for all regression result. Note that if there are
   *                              multiple constant variables in `xColumns` and the user wants to include a
   *                              constant variable, it is recommended to set both `shouldIgnoreConstants`
   *                              and `shouldIntercept` to be true. Default false.
   * @param constantErrorBound    The error bound on (|observations| * variance) to determine if a variable is constant.
   *                              A variable will be considered as a constant c if and only if the sum of squared
   *                              differences to c is less than the error bound. Default is 1.0E-12.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate beta, intercept, stdErr,
   *         t-stats, and r etc.
   */
  def OLSRegression(
    yColumn: String,
    xColumns: Seq[String],
    weightColumn: String = null,
    shouldIntercept: Boolean = true,
    shouldIgnoreConstants: Boolean = false,
    constantErrorBound: Double = 1.0E-12
  ): SummarizerFactory = OLSRegressionSummarizerFactory(
    yColumn,
    xColumns.toArray,
    weightColumn,
    shouldIntercept,
    shouldIgnoreConstants,
    constantErrorBound
  )

  @PythonApi(until = "0.2.5")
  private def OLSRegression(
    yColumn: String,
    xColumns: Seq[String],
    weightColumn: String,
    shouldIntercept: Boolean,
    shouldIgnoreConstants: Boolean
  ): SummarizerFactory =
    OLSRegression(
      yColumn = yColumn,
      xColumns = xColumns,
      weightColumn,
      shouldIntercept = shouldIntercept,
      shouldIgnoreConstants = shouldIgnoreConstants
    )

  @PythonApi(until = "0.2.1")
  private def OLSRegression(
    yColumn: String,
    xColumns: Seq[String],
    weightColumn: String,
    shouldIntercept: Boolean
  ): SummarizerFactory = OLSRegression(
    yColumn = yColumn,
    xColumns = xColumns,
    weightColumn,
    shouldIntercept = shouldIntercept,
    shouldIgnoreConstants = false
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
  def quantile(column: String, p: Seq[Double]): SummarizerFactory =
    QuantileSummarizerFactory(column, p.toArray)

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
  def compose(summarizers: SummarizerFactory*): SummarizerFactory = {
    summarizers.partition(_.isInstanceOf[OverlappableSummarizerFactory]) match {
      case (Seq(), nonOverlappables) =>
        nonOverlappables.reduce(CompositeSummarizerFactory)
      case (overlappables, Seq()) =>
        overlappables
          .map(_.asInstanceOf[OverlappableSummarizerFactory])
          .reduce(OverlappableCompositeSummarizerFactory)
      case _ =>
        throw new IllegalArgumentException(
          s"Can't compose overlappable and non-overlappable summarizers."
        )
    }
  }

  /**
   * Return a summarizer which outputs a single column, `stack`, which contains the results of the summarizer
   * in the order that they were provided.
   *
   * The summarizers must contain identical schemas for this to work.
   * Each summarizer produces one row in the output array.
   *
   * For instance:
   * {{{
   * val predicate1: Int => Boolean = id => id == 3
   * val predicate2: Int => Boolean = id => id == 7
   * stack(
   *   Summarizers.sum("price").where(predicate1)("id"),
   *   Summarizers.sum("price").where(predicate2)("id"),
   *   Summarizers.sum("price")
   * )
   * }}}
   *
   * @param summarizers the summarizers to stack into an array..
   * @return a [[SummarizerFactory]] which produces an Array from the results of the summarizers.
   */
  def stack(summarizers: SummarizerFactory*): SummarizerFactory = {
    StackSummarizerFactory(summarizers)
  }

  /**
   * Performs single exponential smoothing over a column. Primes the EMA by maintaining two EMAs, one over the series
   * (0.0, x_1, x_2, ...) and one over the series (0.0, 1.0, 1.0, ...). For Convolution and Core, the injected
   * zero-valued term is set to be primingPeriods before the first term x_1. For Legacy, it is set to be at time 0.
   *
   * For Core, the smoothed series is the result of dividing each element in the EMA of the first series by the
   * element at the same index in the second series. For Convolution, the smoothed series is simply the EMA of the
   * first series.
   *
   * Calculates EMA as a convolution between the exponential function and the series. Since the series is discrete, it
   * is necessary to interpolate values between rows by specifying the `interpolation`, which
   * supports CurrentPoint, LinearInterpolation, and PreviousPoint interpolations.
   *
   * More concretely, the primary EMA is calculated as follows: suppose we have a time series
   * X = ((x_1, t_1), (x_2, t_2), ..., (x_n, t_n)).
   *
   * For CurrentPoint:
   * <pre><code>(EMA<sub>p</sub> (X))<sub>i</sub> = decay(t<sub>i-1</sub>, t<sub>i</sub>)) (EMA<sub>p</sub> (X))<sub>i-1</sub> + (1 - decay(t<sub>i-1</sub>, t<sub>i</sub>)) x<sub>i</sub> </code>
   * </pre>
   *
   * For LinearInterpolation:
   * <pre><code>(EMA<sub>p</sub> (X))<sub>i</sub> = decay(t<sub>i-1</sub>, t<sub>i</sub>)) (EMA<sub>p</sub> (X))<sub>i-1</sub> +
   * (interpolateDecay - decay(t<sub>i-1</sub>, t<sub>i</sub>)) x<sub>i-1</sub> + (1 - interpolateDecay) x<sub>i</sub></code>
   * </pre>
   *
   * For PreviousPoint:
   * <pre><code>(EMA<sub>p</sub> (X))<sub>i</sub> = decay(t<sub>i-1</sub>, t<sub>i</sub>)) (EMA<sub>p</sub> (X))<sub>i-1</sub> + (1 - decay(t<sub>i-1</sub>, t<sub>i</sub>)) x<sub>i-1</sub> </code>
   * </pre>
   *
   * with the initial conditions
   * <pre><code>(EMA<sub>p</sub> (X))<sub>0</sub> = 0.0, t<sub>0</sub> = t<sub>1</sub> - primingPeriods </code>
   * </pre>
   * and where <pre><code>decay(t<sub>i-1</sub>, t<sub>i</sub>)</code></pre>
   * is the decay between the timestamps jointly specified by timestampsToPeriods and alpha, i.e.
   * <pre><code>decay(t<sub>i-1</sub>, t<sub>i</sub>) = 1 - exp(timestampsToPeriods(t<sub>i-1</sub>, t<sub>i</sub>) * ln(1 - alpha)) </code>
   * </pre>
   *
   * For LinearInterpolation, interpolateDecay is calculated as follows:
   * <pre><code>interpolateDecay =  (1 - decay) / (-timestampsToPeriods(t<sub>i-1</sub>, t<sub>i</sub>) * ln(1 - alpha)) </code>
   * </pre>
   *
   * The auxiliary EMA is calculated as
   * <pre><code>(EMA<sub>a</sub> (X))<sub>i</sub> = decay(t<sub>i-1</sub>, t<sub>i</sub>) (EMA<sub>a</sub> (X))<sub>i-1</sub> + (1 - decay(t<sub>i-1</sub>, t<sub>i</sub>)) </code>
   * </pre>
   * with the same initial conditions.
   *
   * For Core, we take
   * <pre><code>(EMA (X))<sub>i</sub> = (EMA<sub>p</sub> (X))<sub>i</sub> / (EMA<sub>a</sub> (X))<sub>i</sub> </code>
   * </pre>
   *
   * For Convolution and Legacy, we simply take
   * <pre><code>(EMA (X))<sub>i</sub> = (EMA<sub>p</sub> (X))<sub>i</sub> </code>
   * </pre>
   *
   * See doc/ema.md for details on different EMA implementations.
   *
   * The output schema is:
   *   - "<xColumn>_ema": [[DoubleType]], the exponential moving average of the rows.
   *
   * @param xColumn             Name of column containing series to be smoothed
   * @param alpha               The proportion by which the average will decay over one period
   *                            A period is a duration of time defined by the function provided for
   *                            timestampsToPeriods. For instance, if the timestamps in the dataset are
   *                            in nanoseconds, and the function provided in timestampsToPeriods is
   *                            (t2 - t1) / nanosecondsInADay, then the summarizer will take the number
   *                            of periods between rows to be the number of days elapsed between their
   *                            timestamps. Default is 0.05.
   * @param timeColumn          Name of column containing the timestamp
   * @param primingPeriods      Parameter used to find the initial decay parameter - taken to be the
   *                            number of periods (defined above) elapsed before the first data point.
   *                            If the convention "legacy" is specified, this value is ignored.
   *                            Default is 1.
   * @param timestampsToPeriods Function that given two timestamps, returns how many periods should be
   *                            considered to have passed between them. Default is 1 day,
   *                            given timestamps in nanoseconds.
   * @param interpolation       Parameter used to determine the interpolation method for intervals
   *                            between two rows. The options are "previous", "linear", and "current".
   *                            Default is "current".
   * @param convention          Parameter used to determine the convolution convention. The options are
   *                            "convolution", "core", and "legacy". Default is "core".
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate the exponentially smoothed
   *         series
   */
  def exponentialSmoothing(
    xColumn: String,
    alpha: Double = 0.05,
    timeColumn: String = TimeSeriesRDD.timeColumnName,
    primingPeriods: Double = 1.0,
    timestampsToPeriods: (Long, Long) => Double = (t1: Long, t2: Long) =>
      (t2 - t1) / (24 * 60 * 60 * 1e9),
    interpolation: String = "current",
    convention: String = "core"
  ): SummarizerFactory =
    ExponentialSmoothingSummarizerFactory(
      xColumn,
      timeColumn,
      alpha,
      primingPeriods,
      timestampsToPeriods,
      ExponentialSmoothingInterpolation.withName(interpolation),
      ExponentialSmoothingConvention.withName(convention)
    )

  /**
   * Finds the exponential weighted moving average over a column. Similar to exponential smoothing, this maintains
   * an EMA for the series (x_1, x_2, ...) as well as the series (1.0, 1.0, ...). The only difference is
   * that this series does not have an initial zero-valued term injected before each series.
   *
   * The weighted i-th value is simply:
   * <pre><code>decay(t<sub>i</sub>, t<sub>n</sub>) x<sub>i</sub></code>
   * </pre>
   * where <pre><code>decay(t<sub>i</sub>, t<sub>n</sub>)</code></pre>
   * is the decay between the timestamps jointly specified by timestampsToPeriods and alpha, i.e.
   * <pre>
   * <code>
   * decay(t<sub>i</sub>, t<sub>n</sub>) = exp(ln(1 - alpha) * (t<sub>i</sub>, t<sub>n</sub>) / durationPerPeriod)
   * </code>
   * </pre>
   * If durationPerPeriod is "constant", then decay is defined as follows:
   * <pre><code>decay(t<sub>i</sub>, t<sub>n</sub>) = exp((n - i) * ln(1 - alpha)) </code>
   * </pre>
   *
   * The primary EMA keeps track of the sum of the weighted series, whereas the auxiliary EMA keeps track of the sum of
   * the weights.
   *
   * Finally, if the convention is "core", we will take
   * <pre>
   * <code>
   * (EMA (X))<sub>i</sub> = (EMA<sub>p</sub> (X))<sub>i</sub> / (EMA<sub>a</sub> (X))<sub>i</sub>
   * </code>
   * </pre>
   * However, if the convention is "legacy", we will simply return
   * <pre>
   * <code>
   * (EMA (X))<sub>i</sub> = (EMA<sub>p</sub> (X))<sub>i</sub>
   * </code>
   * </pre>
   *
   * See doc/ema.md for details on different EMA implementations.
   *
   * @param xColumn           Name of column containing series to compute the EMA
   * @param alpha             Parameter setting the decay rate of the average. The default is 0.05.
   * @param timeColumn        Name of column containing the timestamp
   * @param durationPerPeriod Duration per period. The option could be "constant" or any string that specifies duration
   *                          like "1d", "1h", "15m" etc. If it is "constant", it will assume that the number of periods
   *                          between rows is constant (c = 1); otherwise, it will use the duration to calculate
   *                          how many periods should be considered to have passed between any two given timestamps.
   *                          The default is "1d".
   * @param convention        Parameter used to determine the convolution convention. The options are "core"
   *                          and "legacy", If it is "core", the primary exponential weighted moving average
   *                          will be further divided by its auxiliary; if it is "legacy", it will return
   *                          the primary exponential weighted moving average. The default is "legacy"
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate the exponential moving average
   */
  def ewma(
    xColumn: String,
    alpha: Double = 0.05,
    timeColumn: String = TimeSeriesRDD.timeColumnName,
    durationPerPeriod: String = "1d",
    convention: String = "legacy"
  ): SummarizerFactory = {
    val constantPeriods = durationPerPeriod.equalsIgnoreCase("constant")
    val durationPerPeriodNanos = if (constantPeriods) {
      1.0
    } else {
      Duration(durationPerPeriod).toNanos.toDouble
    }

    ExponentialWeightedMovingAverageSummarizerFactory(
      xColumn,
      timeColumn,
      alpha,
      (t1: Long, t2: Long) => (t2 - t1) / durationPerPeriodNanos,
      constantPeriods,
      ExponentialWeightedMovingAverageConvention.withName(convention)
    )
  }

  /**
   * Calculates the exponential moving average given a specified half life. Supports the same default behaviors as the
   * previous in-house implementation.
   *
   * See exponentialSmoothing for an explanation of how the EMA is calculated.
   * See doc/ema.md for details on different EMA implementations.
   *
   * The output schema is:
   *   - "<xColumn>_ema": [[DoubleType]], the exponential moving average of the rows.
   *
   * @param xColumn          Name of column containing series to compute the EMA
   * @param timeColumn       Name of column containing the timestamp
   * @param halfLifeDuration String that represents the half life duration.
   * @param interpolation    Parameter used to determine the interpolation method for intervals between
   *                         two rows. The options are "previous", "linear", and "current". Default is
   *                         "previous".
   * @param convention       Parameter used to determine the convolution convention. The options are
   *                         "convolution", "core", and "legacy". Default is "legacy".
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate the exponential moving average
   */
  def emaHalfLife(
    xColumn: String,
    halfLifeDuration: String,
    timeColumn: String = TimeSeriesRDD.timeColumnName,
    interpolation: String = "previous",
    convention: String = "legacy"
  ): SummarizerFactory = {
    val halfLifeDurationNanos = Duration(halfLifeDuration).toNanos

    // Set alpha and timestampsToPeriods accordingly to set the half life.
    // primingPeriods is set to 0 to mimic the previous in-house implementation's behavior.
    ExponentialSmoothingSummarizerFactory(
      xColumn,
      timeColumn,
      0.5,
      0,
      (t1, t2) => (t2 - t1) / halfLifeDurationNanos.toDouble,
      ExponentialSmoothingInterpolation.withName(interpolation),
      ExponentialSmoothingConvention.withName(convention)
    )
  }

  /**
   * Calculates the min for a column.
   *
   * The output schema is:
   *   - "<column>_min": Type of input column
   *
   * @param column The column expected to calculate the min.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the min.
   */
  def min(column: String): SummarizerFactory =
    ExtremeSummarizerFactory(column, ExtremeSummarizerType.Min)

  /**
   * Calculates the max for a column.
   *
   * The output schema is:
   *   - "<column>_max": Type of input column
   *
   * @param column The column expected to calculate the max.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the max.
   */
  def max(column: String): SummarizerFactory =
    ExtremeSummarizerFactory(column, ExtremeSummarizerType.Max)

  /**
   * Calculates the product for a column.
   *
   * The output schema is:
   *   - "<column>_product": [[DoubleType]], the product of the rows.
   *
   * @param column Name of column for which to calculate the product.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the product.
   */
  def product(column: String): SummarizerFactory =
    ProductSummarizerFactory(column)

  /**
   * Calculates the dot product for two columns.
   *
   * The output schema is:
   *   - "<xColumn>_<yColumn>_dotProduct": [[DoubleType]], the dot product of the two columns.
   *
   * @param xColumn Name of the first column.
   * @param yColumn Name of the second column.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the dot product.
   */
  def dotProduct(xColumn: String, yColumn: String): SummarizerFactory =
    DotProductSummarizerFactory(xColumn, yColumn)

  /**
   * Calculates the geometric mean for a column.
   *
   * The output schema is:
   *   - "<column>_geometricMean": [[DoubleType]], the geometric mean of the rows.
   *
   * @param column Name of column for which to calculate the geometric mean.
   * @return a [[SummarizerFactory]] which could provide a summarizer to calculate the geometric mean.
   */
  def geometricMean(column: String): SummarizerFactory =
    GeometricMeanSummarizerFactory(column)

  /**
   * Calculates the skewness for a column. This is the third standardized moment.
   *
   * The output schema is:
   *   - "<column>_skewness": [[DoubleType]]
   *
   * @param column Name of the column to calculate skewness.
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate skewness.
   */
  def skewness(column: String): SummarizerFactory =
    StandardizedMomentSummarizerFactory(
      column,
      StandardizedMomentSummarizerType.Skewness
    )

  /**
   * Calculates the excess kurtosis for a column. This is the fourth standardized moment subtracted by 3.
   *
   * The output schema is:
   *   - "<column>_kurtosis": [[DoubleType]]
   *
   * @param column Name of the column to calculate kurtosis.
   * @return a [[SummarizerFactory]] which provides a summarizer to calculate kurtosis.
   */
  def kurtosis(column: String): SummarizerFactory =
    StandardizedMomentSummarizerFactory(
      column,
      StandardizedMomentSummarizerType.Kurtosis
    )
}
