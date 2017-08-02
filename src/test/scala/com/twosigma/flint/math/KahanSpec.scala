/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.math

import java.math.BigDecimal

import com.twosigma.flint.util.Timer
import org.scalatest.FlatSpec

import scala.util.Random

class KahanSpec extends FlatSpec {

  "Kahan" should "sum correctly in wiki example" in {
    val kahan = new Kahan()
    var i = 0
    while (i < 1000) {
      kahan.add(1.0)
      kahan.add(1.0e100)
      kahan.add(1.0)
      kahan.add(-1.0e100)
      i += 1
    }

    assert(kahan.value === 2000.0)
  }

  it should "sum correctly for constants of Double(s)" in {
    val kahan = new Kahan()
    val x = 1000.0002
    var sum = 0.0
    val bigDecimal = new BigDecimal(x)
    var bigDecimalSum = new BigDecimal(0.0)
    var i = 0
    while (i < (Int.MaxValue >> 5)) {
      sum += x
      kahan.add(x)
      bigDecimalSum = bigDecimalSum.add(bigDecimal)
      i += 1
    }
    assert(
      Math.abs(
        bigDecimalSum
          .subtract(new BigDecimal(kahan.value))
          .doubleValue()
      ) < 1.0e-5
    )

    assert(
      Math.abs(
        bigDecimalSum
          .subtract(new BigDecimal(sum))
          .doubleValue()
      ) > 1.0
    )
  }

  it should "subtract correctly" in {
    val kahan1 = new Kahan()
    val kahan2 = new Kahan()
    val x = 1000.0002
    var i = 0
    while (i < (Int.MaxValue >> 5)) {
      kahan1.add(x)
      kahan2.add(x)
      kahan2.add(x)
      i += 1
    }
    kahan2.subtract(kahan1)
    assert(kahan2.value === kahan1.value)
  }
}
