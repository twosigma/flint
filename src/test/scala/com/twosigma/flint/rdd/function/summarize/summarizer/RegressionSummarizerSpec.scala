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

package com.twosigma.flint.rdd.function.summarize.summarizer

import breeze.linalg.{ DenseVector, DenseMatrix }
import org.scalatest.FlatSpec

class RegressionSummarizerSpec extends FlatSpec {

  "RegressionSummarizer" should "transform from RegressRow correctly" in {
    val x: Array[RegressionRow] = Array(
      RegressionRow(time = 0L, x = Array(1d, 2d), y = 3d, weight = 4d),
      RegressionRow(time = 0L, x = Array(4d, 5d), y = 6d, weight = 16d)
    )

    val (response1, predictor1, yw1) = RegressionSummarizer.transform(x, shouldIntercept = true, isWeighted = true)
    assert(response1.equals(DenseMatrix(Array(2d, 2d, 4d), Array(4d, 16d, 20d))))
    assert(predictor1.equals(DenseVector(Array(6d, 24d))))
    assert(yw1.deep == Array((3d, 4d), (6d, 16d)).deep)

    val (response2, predictor2, yw2) = RegressionSummarizer.transform(x, shouldIntercept = true, isWeighted = false)
    assert(response2.equals(DenseMatrix(Array(1d, 1d, 2d), Array(1d, 4d, 5d))))
    assert(predictor2.equals(DenseVector(Array(3d, 6d))))
    assert(yw2.deep == Array((3d, 1d), (6d, 1d)).deep)

    val (response3, predictor3, yw3) = RegressionSummarizer.transform(x, shouldIntercept = false, isWeighted = true)
    assert(response3.equals(DenseMatrix(Array(2d, 4d), Array(16d, 20d))))
    assert(predictor3.equals(DenseVector(Array(6d, 24d))))
    assert(yw3.deep == Array((3d, 4d), (6d, 16d)).deep)

    val (response4, predictor4, yw4) = RegressionSummarizer.transform(x, shouldIntercept = false, isWeighted = false)
    assert(response4.equals(DenseMatrix(Array(1d, 2d), Array(4d, 5d))))
    assert(predictor4.equals(DenseVector(Array(3d, 6d))))
    assert(yw4.deep == Array((3d, 1d), (6d, 1d)).deep)
  }
}
