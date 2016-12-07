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

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object TreeReduce {

  /**
   * Reduces the elements from an rdd, in a multi-level tree pattern, in order, i.e.,
   *   - `f` will be applied in each partition in the ordering of elements in that partition
   *   - `f` will be applied between partitions respect to the ordering of partitions' indices.
   *
   * @param rdd   The rdd expected to reduce
   * @param f     The operator that merge two elements
   * @param depth The suggested depth of the tree (default: 2)
   * @return the reduced result.
   * @see [[org.apache.spark.rdd.RDD#treeReduce]]
   */
  def apply[T: ClassTag](
    rdd: RDD[T]
  )(
    f: (T, T) => T,
    depth: Int = 2
  ): T = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")

    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }

    val partiallyReduced = rdd.mapPartitions(it => Iterator(reducePartition(it)))

    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(f(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }

    TreeAggregate(partiallyReduced)(Option.empty[T], op, op, depth).getOrElse(
      sys.error("Empty collection.")
    )
  }

}
