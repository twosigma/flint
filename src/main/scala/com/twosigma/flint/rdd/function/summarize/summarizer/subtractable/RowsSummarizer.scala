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
import java.util.ArrayDeque
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A summarizer that puts all input values into an Array in the order they are added.
 *
 * Using ArrayDeque instead of LinkedList is because its better performance `toArray` operation.
 */
case class RowsSummarizer[@specialized V: ClassTag]()
  extends LeftSubtractableSummarizer[V, ArrayDeque[V], Array[V]] {

  override def zero(): ArrayDeque[V] = new ArrayDeque[V]()

  override def add(u: ArrayDeque[V], t: V): ArrayDeque[V] = {
    u.addLast(t)
    u
  }

  override def subtract(u: ArrayDeque[V], t: V): ArrayDeque[V] = {
    u.removeFirst()
    u
  }

  override def merge(u1: ArrayDeque[V], u2: ArrayDeque[V]): ArrayDeque[V] = {
    u1.addAll(u2)
    u1
  }

  override def render(u: ArrayDeque[V]): Array[V] = {
    val values = new Array[V](u.size)
    System.arraycopy(u.toArray, 0, values, 0, u.size)
    values
  }
}

/**
 * The reason we need this class instead of using RowsSummarizer[InternalRow] directly is because
 * its performance is much better. The performance improvement is mainly achieved by avoiding using
 * java.lang.reflect.Array.create to create new array instance which is ~ 10x slower than the
 * native java array creation.
 */
case class InternalRowsSummarizer()
  extends LeftSubtractableSummarizer[InternalRow, ArrayDeque[InternalRow], Array[InternalRow]] {

  override def zero(): ArrayDeque[InternalRow] = new ArrayDeque[InternalRow]()

  override def add(u: ArrayDeque[InternalRow], t: InternalRow): ArrayDeque[InternalRow] = {
    u.addLast(t)
    u
  }

  override def subtract(u: ArrayDeque[InternalRow], t: InternalRow): ArrayDeque[InternalRow] = {
    u.removeFirst()
    u
  }

  override def merge(u1: ArrayDeque[InternalRow], u2: ArrayDeque[InternalRow]): ArrayDeque[InternalRow] = {
    u1.addAll(u2)
    u1
  }

  override def render(u: ArrayDeque[InternalRow]): Array[InternalRow] = {
    val values = new Array[InternalRow](u.size)
    System.arraycopy(u.toArray, 0, values, 0, u.size)
    values
  }
}
