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

package com.twosigma.flint.rdd

/**
 * Type of key partitioning and ordering in RDD[(K, V)] where a tuple represents a key-value pair.
 */
sealed trait KeyPartitioningType

object KeyPartitioningType {

  /**
   * An rdd is not `Sorted`.
   */
  case object UnSorted extends KeyPartitioningType

  /**
   * An rdd is considered to be sorted iff
   *   - all keys of rows in the k-th partition are less or equal than those of (k + 1)-th partition for all k;
   *   - all rows of any partition are also sorted by their keys.
   */
  case object Sorted extends KeyPartitioningType

  /**
   * An rdd is considered to be sorted and normalized iff
   *   - all keys of rows in the k-th partition are strictly less than those of (k + 1)-th partition for all k,
   *     i.e. there is no key existing multiple partitions;
   *   - all rows of any partition are also sorted by their keys.
   */
  case object NormalizedSorted extends KeyPartitioningType

  def apply(isSorted: Boolean, isNormalized: Boolean): KeyPartitioningType =
    if (isSorted) {
      if (isNormalized) {
        NormalizedSorted
      } else {
        Sorted
      }
    } else {
      UnSorted
    }
}
