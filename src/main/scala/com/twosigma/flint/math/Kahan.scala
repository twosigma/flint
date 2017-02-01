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

package com.twosigma.flint.math

/**
 * This uses the [[http://en.wikipedia.org/wiki/Kahan_summation_algorithm Kahan summation algorithm]] to minimize errors when
 * adding a large amount of doubles.
 *
 * @note it is mutable and not thread safe.
 */
object Kahan {
  def apply(): Kahan = new Kahan(0.0, 0.0)
}

private[flint] class Kahan(var sum: Double, var error: Double) extends Serializable {

  def getValue(): Double = sum

  def multiply(x: Double): Kahan = {
    sum *= x
    error *= x
    this
  }

  def add(v: Double): Kahan = {
    val y = v - error
    val t = sum + y
    error = (t - sum) - y
    sum = t
    this
  }

  def add(other: Kahan): Kahan = {
    add(other.sum)
    add(other.error)
    this
  }

  def subtract(other: Kahan): Double = {
    var subtracted = sum - other.sum
    subtracted += other.error - error
    subtracted
  }
}
