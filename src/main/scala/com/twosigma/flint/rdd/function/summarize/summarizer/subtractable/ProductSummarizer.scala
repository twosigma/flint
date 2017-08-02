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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

import com.twosigma.flint.math.Kahan

/**
 * @param count              The count of rows
 * @param zeroCount          The count of zero-valued rows
 * @param isPositive         Whether or not the current product is positive.
 * @param sumOfLogs          The sum of the log of rows
 */
case class ProductState(
  var count: Long,
  var zeroCount: Long,
  var isPositive: Boolean,
  sumOfLogs: Kahan
)

/**
 * To minimize floating point errors, we keep the sum of the logs of the values as opposed to directly storing the
 * product at each iteration. This is because the following expressions are equivalent:
 * exp(ln x_1 + ln x_2 + ln x_3 + ...) = x_1 * x_2 * x_3 * ...
 *
 * We must keep track of zeroes as having any zeroes will result in a product of zero. Similarly, we also must
 * keep track of the sign of the product, as this is flipped for every negative value encountered. This is necessary
 * with the sum of logs approach as we cannot take the log of a negative number.
 */
class ProductSummarizer extends LeftSubtractableSummarizer[Double, ProductState, Double] {
  def zero(): ProductState = ProductState(0L, 0L, true, new Kahan())

  def add(u: ProductState, data: Double): ProductState = {
    if (data == 0.0) {
      u.zeroCount += 1L
    } else if (data < 0.0) {
      u.sumOfLogs.add(Math.log(-data))
      u.isPositive = !u.isPositive
    } else {
      u.sumOfLogs.add(Math.log(data))
    }
    u.count += 1L
    u
  }

  def subtract(u: ProductState, data: Double): ProductState = {
    require(u.count != 0L)
    if (data == 0.0) {
      u.zeroCount -= 1L
    } else if (data < 0.0) {
      u.sumOfLogs.add(-Math.log(-data))
      u.isPositive = !u.isPositive
    } else {
      u.sumOfLogs.add(-Math.log(data))
    }
    u.count -= 1L
    u
  }

  def merge(u1: ProductState, u2: ProductState): ProductState = {
    u1.zeroCount += u2.zeroCount
    u1.sumOfLogs.add(u2.sumOfLogs)
    u1.count += u2.count
    u1.isPositive = u1.isPositive == u2.isPositive
    u1
  }

  def render(u: ProductState): Double = {
    if (u.count == 0L) {
      Double.NaN
    } else if (u.zeroCount > 0L) {
      0.0
    } else if (u.isPositive) {
      Math.exp(u.sumOfLogs.value)
    } else {
      -Math.exp(u.sumOfLogs.value)
    }
  }
}
