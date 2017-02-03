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

import org.apache.spark.Partition

/**
 * In Spark 2, Partition.equals() uses Serializable.equals(). This breaks equality comparision of OrderedRDDPartition
 * in tests. We need to investigate why Serializable.equals() returns false for two OrderedRDDPartitions, but for
 * now, we are going to use this class for equality comparison.
 */
case class Split(override val index: Int) extends Partition {
  // http://stackoverflow.com/questions/11890805/hashcode-of-an-int
  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = other match {
    case that: Partition => this.index == that.index
    case _ => false
  }
}
