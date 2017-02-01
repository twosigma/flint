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

import scala.reflect.ClassTag

/**
 * A summarizer that puts all input values into a vector in the order they are added.
 */
case class RowsSummarizer[V: ClassTag]() extends LeftSubtractableSummarizer[V, Vector[V], Vector[V]] {
  override def zero(): Vector[V] = Vector[V]()
  override def add(u: Vector[V], t: V): Vector[V] = u :+ t
  override def subtract(u: Vector[V], t: V): Vector[V] = u.drop(1)
  override def merge(u1: Vector[V], u2: Vector[V]): Vector[V] = u1 ++ u2
  override def render(u: Vector[V]): Vector[V] = u
}
