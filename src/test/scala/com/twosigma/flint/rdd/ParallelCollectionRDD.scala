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

package com.twosigma.flint.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }

import scala.reflect.ClassTag

/**
 * Similar to Spark's ParallelCollectionRDD, but takes Array[Array[T]] instead of Seq[T]
 */

case class ParallelCollectionRDDPartition[T: ClassTag](
  override val index: Int,
  values: Seq[T]
) extends Partition

class ParallelCollectionRDD[T: ClassTag](
  sc: SparkContext,
  @transient data: Seq[Seq[T]]
) extends RDD[T](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    split.asInstanceOf[ParallelCollectionRDDPartition[T]].values.iterator

  override protected def getPartitions: Array[Partition] =
    data.zipWithIndex.map {
      case (d, index) =>
        ParallelCollectionRDDPartition(index, d)
    }.toArray
}
