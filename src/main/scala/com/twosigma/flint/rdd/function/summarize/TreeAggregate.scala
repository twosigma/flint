/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.rdd.function.summarize

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

protected[flint] object TreeAggregate {

  /**
   * Aggregates the elements from an rdd, in a multi-level tree pattern, in order, i.e.,
   *   - `seqOp` will be applied in each partition in the ordering of elements in that partition
   *   - `combOp` will be applied between partitions respect to the ordering of partitions' indices.
   *
   * @param rdd       The rdd expected to aggregate
   * @param zeroValue The zero value
   * @param seqOp     The sequential operator expected to apply in each partition
   * @param combOp    The combine operator expected to merge/combine the partial results from each partition
   * @param depth     The suggested depth of the tree (default: 2)
   * @return the aggregated result if the `rdd` is non-empty, otherwise `zeroValue`.
   * @see [[org.apache.spark.rdd.RDD#treeAggregate]]
   */
  def apply[T: ClassTag, U: ClassTag](
    rdd: RDD[T]
  )(
    zeroValue: U,
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2
  ): U = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    if (rdd.partitions.length == 0) {
      zeroValue
    } else {
      val aggregatePartition = (it: Iterator[T]) => it.foldLeft(zeroValue)(seqOp)
      // The following is an RDD[(Int, U)] where the key of type Int is simply the partition index.
      var partiallyAggregated = rdd.mapPartitionsWithIndex {
        (i, iter) => Iterator((i, aggregatePartition(iter)))
      }
      var numPartitions = partiallyAggregated.partitions.length
      // TODO: potentially could use different scale. Here, the trade-off is between the number
      //       of times of shuffling in the while-loop later and the memory pressure for each
      //       executor to perform (sorted) reduce op.
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)

      // In RDD.treeAggregate(...), it will collect all U(s) (after the number of
      // partitions is sufficient small) back to driver to reduce wall-clock time.
      // However, here it will keep aggregating in a multi-level tree pattern until the
      // number of partitions is 1.
      while (numPartitions > 1) {
        numPartitions /= scale
        val curNumPartitions = math.max(numPartitions, 1)
        partiallyAggregated = partiallyAggregated.map {
          // Comparing to RDD.treeAggregate(...), the hash function used here is (i / curNumPartitions)
          // instead of (i % curNumPartitions).
          case (i, u) => (i / curNumPartitions, (i, u))
        }.groupByKey(new HashPartitioner(curNumPartitions)).map {
          // k is simply (i / curNumPartitions) from the above hash function.
          case (k, iter) =>
            // Note that we have to sort the U(s) by their original order before reduce op.
            (k, iter.toArray.sortBy(_._1).map(_._2).reduce(combOp))
        }
      }

      // The final step of tree aggregation should only have one single partition.
      require(partiallyAggregated.partitions.length == 1)

      // Aggregate in the executor side instead of driver.
      val reduced = partiallyAggregated.mapPartitions {
        iter => Iterator(iter.toArray.sortBy(_._1).map(_._2).reduce(combOp))
      }.collect()

      require(reduced.length <= 1)

      reduced.headOption.getOrElse(zeroValue)
    }
  }
}
