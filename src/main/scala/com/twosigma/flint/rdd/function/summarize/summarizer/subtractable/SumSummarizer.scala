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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

case class SumSummarizer[T](
  implicit
  n: Numeric[T]
) extends LeftSubtractableSummarizer[T, T, T] {
  def zero(): T = n.zero
  def add(u: T, t: T): T = n.plus(u, t)
  def subtract(u: T, t: T): T = n.minus(u, t)
  def merge(u1: T, u2: T): T = n.plus(u1, u2)
  def render(u: T): T = u
}
