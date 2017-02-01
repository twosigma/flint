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

import org.apache.spark.rdd.RDD
import breeze.linalg.{ DenseMatrix, DenseVector }

object OLSMultipleLinearRegression {

  /**
   * Build the linear regression model with the input data.
   *
   * @param input RDD of (label, weight, array of features) tuples. Each pair describes a row of
   * the data matrix X, the weight, as well as the corresponding right hand side label y.
   * @param intercept Whether to use intercept (default true).
   * @return the linear regression model.
   */
  def regression(input: RDD[WeightedLabeledPoint], intercept: Boolean = true): LinearRegressionModel = {
    // Try to get the number of columns
    val nCols = if (intercept) {
      input.first.features.length + 1
    } else {
      input.first.features.length
    }

    val (xx, xy, swx, srwsl, ssrw, wsl, sw, n, lw) = input.treeAggregate((
      new DenseMatrix[Double](nCols, nCols), // 1. Calculate a k-by-k matrix X^TX.
      new DenseVector[Double](nCols), // 2. Calculate a k-dimension vector X^Ty.
      new DenseVector[Double](nCols), // 3. Calculate a k-dimension vector of weighted sum of X.
      0.0, // 4. Calculate the square root weighted sum of labels.
      0.0, // 5. Calculate the sum of square root of weights.
      0.0, // 6. Calculate the weighted sum of labels.
      0.0, // 7. Calculate the sum of weights.
      0: Long, // 8. Calculate the length of input.
      0.0 // 9. Calculate sum of log weights
    ))(
      // U is a pair of matrix and vector and v is a WeightedLabeledPoint.
      seqOp = (U, v) => {
      // Append 1.0 at the head for calculating intercept.
      val x = if (intercept) {
        DenseVector.vertcat(DenseVector(1.0), v.features)
      } else {
        v.features
      }
      val wx = x * v.weight
      val sqrtW = Math sqrt v.weight
      // Unfortunately, breeze.linalg.DenseVector does not support tensor product.
      (U._1 += wx.asDenseMatrix.t * x.asDenseMatrix,
        U._2 += wx * v.label,
        U._3 += wx,
        U._4 + v.label * sqrtW,
        U._5 + sqrtW,
        U._6 + v.label * v.weight,
        U._7 + v.weight,
        U._8 + 1,
        U._9 + math.log(v.weight))
    }, combOp = (U1, U2) => (
      U1._1 += U2._1,
      U1._2 += U2._2,
      U1._3 += U2._3,
      U1._4 + U2._4,
      U1._5 + U2._5,
      U1._6 + U2._6,
      U1._7 + U2._7,
      U1._8 + U2._8,
      U1._9 + U2._9
    )
    )
    LinearRegressionModel(input, intercept, n, (xx + xx.t) :/ 2.0, xy, swx, srwsl, ssrw, wsl, sw, lw)
  }
}
