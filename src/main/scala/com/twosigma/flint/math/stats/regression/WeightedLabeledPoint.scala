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

package com.twosigma.flint.math.stats.regression

import breeze.linalg.DenseVector
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

case class WeightedLabeledPoint(label: Double, weight: Double, features: DenseVector[Double]) {
  /**
   * Return a string representation of WeightedLabeledPoint which could be easily parsed.
   */
  override def toString: String = s"$label,$weight,${features.toArray.mkString(",")}"
}

object WeightedLabeledPoint {
  /**
   * Parse a given string into WeightedLabeledPoint.
   *
   * @param s The string expected to parse.
   * @param delimiter The delimiter expected to split the string into tokens.
   * @return this WeightedLabeledPoint.
   */
  def parse(s: String, delimiter: String = ","): WeightedLabeledPoint = {
    val a = s.split(delimiter).map(_.toDouble)
    WeightedLabeledPoint(a(0), a(1), new DenseVector[Double](a, 2, 1, a.length - 2))
  }

  /**
   * Converts the given RDD of WeightedLabeledPoint to a pair of arrays where the left hand-side
   * array represents a matrix of features scaled with square root of weights respectively and
   * the right hand-side array represent a vector of labels scaled with square root of weights
   * respectively as well.
   *
   * @param data The RDD of WeightedLabeledPoint expected to convert.
   * @param intercept Whether to use intercept (default true). If it is true, the first column
   * of resulting matrix will be square root of weights.
   * @return a pair of arrays representing weighted features and labels.
   */
  def toArrays(
    data: RDD[WeightedLabeledPoint],
    intercept: Boolean = true
  ): (Array[Array[Double]], Array[Double]) = {
    val t = data.map { point =>
      val w = Math.sqrt(point.weight)
      val p = point.features * w
      if (intercept) {
        (w +: p.toArray, point.label * w)
      } else {
        (p.toArray, point.label * w)
      }
    }.collect().unzip
    (t._1.toArray, t._2.toArray)
  }

  /**
   * Generate an RDD of random WeightedLabeledPoint for the given parameters. For the given weights
   * and intercept, the feature and the weight of each point is a vector containing i.i.d.
   * samples ~ N(0.0, 1.0), and the label of the point is simply
   * <p><code> label = feature &middot; weights + intercept + errorScalar * N(0.0, 1.0) </code>
   * </p>
   *
   * @param sc The SparkContext used to create the RDD.
   * @param weights The weights.
   * @param intercept The intercept.
   * @param numRows The number of WeightedLabeledPoint in the RDD.
   * @param numPartitions The number of partitions in the RDD.
   * @param errorScalar The scalar of the error.
   * @param seed The random seed.
   * @return an RDD of random WeightedLabeledPoint.
   */
  def generateSampleData(sc: SparkContext, weights: DenseVector[Double], intercept: Double,
    numRows: Long = 100L, numPartitions: Int = 4, errorScalar: Double = 1.0,
    seed: Long = 1L): RDD[WeightedLabeledPoint] = {
    val len = weights.length + 2
    // The last entry will serve as the weight of point and the second last entry will serve
    // as noisy of the label.
    val data = RandomRDDs.normalVectorRDD(sc, numRows, len, numPartitions, seed)
    data.map { d =>
      val fw = d.toArray
      val x = new DenseVector(fw.dropRight(2))
      WeightedLabeledPoint(
        weights.dot(x) + intercept + errorScalar * fw(len - 2),
        Math.abs(fw(len - 1)) + 0.5, x
      )
    }
  }
}
