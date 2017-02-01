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

import org.scalatest.FlatSpec
import breeze.linalg.DenseVector

class WeightedLabeledPointSpec extends FlatSpec {
  "The WeightedLabeledPoint" should "print correctly" in {
    val point = WeightedLabeledPoint(1.0, 2.0, DenseVector(3.0, 4.0))
    assert(point.toString() == "1.0,2.0,3.0,4.0")
  }
}
