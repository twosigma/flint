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

package com.twosigma.flint.rdd.function.summarize.summarizer

import scala.reflect.ClassTag

/**
 * Takes multiple summarizers, and provided that they all render the same type,
 * puts them together in an array.
 * Similar to CompositeSummarizer but makes the row tall, rather than wide.
 */
case class StackSummarizer[T, U, V: ClassTag](summarizers: Seq[Summarizer[T, U, V]])
  extends Summarizer[T, Seq[U], Seq[V]] {
  override def zero(): Seq[U] = summarizers.map(_.zero())

  override def add(u: Seq[U], t: T): Seq[U] = u.zip(summarizers).map { case (u, s) => s.add(u, t) }

  override def merge(u1: Seq[U], u2: Seq[U]): Seq[U] = u1.zip(u2).zip(summarizers)
    .map { case ((u1, u2), s) => s.merge(u1, u2) }

  override def render(u: Seq[U]): Seq[V] = u.zip(summarizers).map { case (u, s) => s.render(u) }
}
