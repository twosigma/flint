/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.sql.function.aggregate

import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.math.{ abs, signum, sqrt }

/**
 * Calculates the weighted mean, weighted deviation, and weighted tstat.
 *
 * Takes every (sample, weight) pair and treats them as if they were written
 * (sign(weight) * sample, abs(weight)).
 *
 * Implemented based on
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Weighted_incremental_algorithm Weighted incremental algorithm]] and
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm Parallel algorithm]]
 * and replaces all "n" with corresponding "SumWeight"
 */

class WeightedMeanTest extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: StructField("weight", DoubleType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("count", LongType) ::
      StructField("sumWeight", DoubleType) ::
      StructField("mean", DoubleType) ::
      StructField("sumSquareOfDiffFromMean", DoubleType) ::
      StructField("sumSquareOfWeights", DoubleType) :: Nil)

  override def dataType: DataType = StructType(StructField("weightedMean", DoubleType) ::
    StructField("weightedStandardDeviation", DoubleType) ::
    StructField("weightedTstat", DoubleType) ::
    StructField("observationCount", LongType) :: Nil)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
    buffer(3) = 0.0
    buffer(4) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val rawValue = input.getAs[Double](0)
    val rawWeight = input.getAs[Double](1)

    // Move the sign from weight to value, keep weight non-negative
    val value = rawValue * signum(rawWeight)
    val weight = abs(rawWeight)

    val count = buffer.getAs[Long](0)
    val sumWeight = buffer.getAs[Double](1)
    val mean = buffer.getAs[Double](2)
    val sumSquareOfDiffFromMean = buffer.getAs[Double](3)
    val sumSquareOfWeights = buffer.getAs[Double](4)

    val temp = weight + sumWeight
    val delta = value - mean
    val R = delta * weight / temp

    buffer(0) = count + 1
    buffer(1) = temp
    buffer(2) = mean + R
    buffer(3) = sumSquareOfDiffFromMean + (sumWeight * delta * R)
    buffer(4) = sumSquareOfWeights + (weight * weight)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val count2 = buffer2.getAs[Long](0)

    if (count2 > 0) {
      val count1 = buffer1.getAs[Long](0)
      val sumWeight1 = buffer1.getAs[Double](1)
      val mean1 = buffer1.getAs[Double](2)
      val sumSquareOfDiffFromMean1 = buffer1.getAs[Double](3)
      val sumSquareOfWeights1 = buffer1.getAs[Double](4)

      val sumWeight2 = buffer2.getAs[Double](1)
      val mean2 = buffer2.getAs[Double](2)
      val sumSquareOfDiffFromMean2 = buffer2.getAs[Double](3)
      val sumSquareOfWeights2 = buffer2.getAs[Double](4)

      val delta = mean2 - mean1

      buffer1(0) = count1 + count2
      buffer1(1) = sumWeight1 + sumWeight2
      // This particular way to calculate mean is chosen based on
      // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
      buffer1(2) = (sumWeight1 * mean1 + sumWeight2 * mean2) / (sumWeight1 + sumWeight2)
      buffer1(3) = sumSquareOfDiffFromMean1 + sumSquareOfDiffFromMean2 +
        delta * delta * sumWeight1 * sumWeight2 / (sumWeight1 + sumWeight2)
      buffer1(4) = sumSquareOfWeights1 + sumSquareOfWeights2
    }
  }

  override def evaluate(buffer: Row): Any = {
    val count = buffer.getAs[Long](0)
    val sumWeight = buffer.getAs[Double](1)
    val mean = buffer.getAs[Double](2)
    val sumSquareOfDiffFromMean = buffer.getAs[Double](3)
    val sumSquareOfWeights = buffer.getAs[Double](4)

    val variance = sumSquareOfDiffFromMean / sumWeight
    val stdDev = sqrt(variance)
    val tStat = sqrt(count.toDouble) * mean / stdDev

    Row(mean, stdDev, tStat, count)
  }

}
