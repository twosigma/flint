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

package com.twosigma.flint.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, Partition, TaskContext }

protected[flint] object PartitionsIterator {
  def apply[T](
    rdd: RDD[T],
    partitions: Seq[Partition],
    context: TaskContext,
    preservesPartitionsOrdering: Boolean = false // FIXME: This is a band-aid which should be fixed.
  ): PartitionsIterator[T] = new PartitionsIterator(rdd, partitions, context, preservesPartitionsOrdering)
}

/**
 * An iterator for records in a sequence of partitions from an [[RDD]].
 *
 * @param rdd                         The [[RDD]] expected to iterate through.
 * @param partitions                  A sequence of [[Partition]] from the given [[RDD]].
 * @param context                     The [[TaskContext]].
 * @param preservesPartitionsOrdering A flag indicates whether the iterator should iterate through
 *                                    partitions in the given order or it should sort them by their indices first.
 *                                    By default, it is false and it will sort first.
 */
protected[flint] class PartitionsIterator[T](
  rdd: RDD[T],
  partitions: Seq[Partition],
  context: TaskContext,
  preservesPartitionsOrdering: Boolean = false // FIXME: This is a band-aid which should be fixed.
) extends BufferedIterator[T] with Logging {

  var _partitions = partitions
  if (!preservesPartitionsOrdering) {
    _partitions = partitions.sortBy(_.index)
  }

  var curIdx = 0
  var curPart: Partition = null
  var curIter: BufferedIterator[T] = null

  private[this] def nextIter() {
    if (curIdx < _partitions.size) {
      val part = _partitions(curIdx)
      logInfo(s"Opening iterator for partition: ${part.index}")
      curIter = rdd.iterator(part, context).buffered
      curPart = part
      curIdx += 1
    } else {
      curIter = null
      curPart = null
      curIdx = -1
    }
  }

  lazy val init = {
    logInfo(s"Beginning to read partitions: ${_partitions}")
    nextIter()
  }

  @annotation.tailrec
  override final def hasNext: Boolean = {
    init
    if (curIdx == -1) false
    else if (curIter.hasNext) true
    else {
      nextIter()
      hasNext
    }
  }

  override def next: T = {
    init
    // Check for the next element and it could update curIter.
    if (hasNext) curIter.next
    else sys.error("hit end of iterator")
  }

  override def head: T = curIter.head

  /**
   * @return the index of partition that the current pointer points to.
   */
  def headPartitionIndex: Int = curPart.index
}
