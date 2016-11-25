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

import com.twosigma.flint.hadoop._
import org.apache.spark.rdd.RDD
import org.apache.spark._

import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag

object Conversion {

  /**
   * Convert a sorted [[org.apache.spark.rdd.RDD]] to an [[OrderedRDD]]. An rdd is considered to be sorted iff
   * all keys of records in the kth partition are less or equal than those of (k + 1)th partition for all possible
   * k and each partition's rows are also sorted by their keys.
   *
   * @param rdd The [[org.apache.spark.rdd.RDD]] of (K, V) tuple(s) expected to convert. Note that the first partition
   *            does not necessarily have tuple(s) with the smallest keys.
   * @return an [[OrderedRDD]].
   */
  def fromSortedRDD[K: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)]
  )(implicit ord: Ordering[K]): OrderedRDD[K, V] = {

    // Get the header information for each partition from the given parent rdd.
    val headers = rdd.mapPartitionsWithIndex {
      case (idx, iter) =>
        Iterator(if (iter.nonEmpty) {
          val firstKey = iter.next._1
          // XXX Try to find the first two distinct keys for every partition. If there is
          // only one key in the whole partition, put the second key as None.
          // A basic assumption we made here is that there are not too many records with the
          // same key within a single partition. Thus, searching the distinct second key should be
          // considered as a ``light weight'' operation.
          Seq(OrderedPartitionHeader(
            // The following partition is not the right type which should be the type of *current*
            // partition. We will convert it back later on.
            OrderedRDDPartition(idx),
            firstKey,
            iter.find(it => ord.gt(it._1, firstKey)).map(_._1)
          ))
        } else {
          Seq.empty
        })
    }.reduce(_ ++ _).map {
      // Convert the partition type back. This is a trick to avoid propagating the partition of
      // parent rdd all around.
      hdr =>
        require(rdd.partitions(hdr.partition.index).index == hdr.partition.index)
        OrderedPartitionHeader(rdd.partitions(hdr.partition.index), hdr.firstKey, hdr.secondKey)
    }

    val rangeDep = RangeDependency.normalize(headers)
    val dependencies = rangeDep.map { d => (d.index, d.parents) }.toMap

    val dep = new NarrowDependency(rdd) {
      override def getParents(partitionId: Int) =
        dependencies.getOrElse(
          partitionId,
          sys.error(s"Unclear dependency for the parents of partition $partitionId.")
        ).map(_.index).sorted
    }
    val splits = rangeDep.map {
      d => RangeSplit(OrderedRDDPartition(d.index).asInstanceOf[Partition], d.range)
    }
    val partitionIndexToParents = rangeDep.map{ d => (d.index, d) }.toMap

    new OrderedRDD[K, V](rdd.sparkContext, splits, Seq(dep))(
      (part, context) => {
        val thisDep = partitionIndexToParents.getOrElse(
          part.index,
          sys.error(s"Unclear dependency for the parents of partition ${part.index}.")
        )
        PeekableIterator(
          OrderedIterator(
            PartitionsIterator(rdd, thisDep.parents, context)
          ).filterByRange(thisDep.range)
        )
      }
    )
  }

  /**
   * Convert a normalized sorted [[org.apache.spark.rdd.RDD]] to an [[OrderedRDD]]. An rdd is considered to be
   * sorted and normalized iff all keys of records in the kth partition are strictly less than
   * those of (k + 1)th partition for all possible k, i.e. there is no a single key existing multiple
   * partitions and each partition's rows are also sorted by their primary keys.
   *
   * @param rdd The [[org.apache.spark.rdd.RDD]] of (K, V) tuple(s) expected to convert. Note that the first partition
   *           does not necessarily have tuple(s) with the smallest keys.
   * @return an [[OrderedRDD]].
   */
  def fromNormalizedSortedRDD[K: Ordering: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): OrderedRDD[K, V] = {
    val partitionToFirstKey = rdd.mapPartitionsWithIndex {
      case (index, iter) =>
        Iterator(if (iter.nonEmpty) Map(index -> iter.next._1) else Map[Int, K]())
    }.reduce(_ ++ _)

    val indexMapping = partitionToFirstKey.toSeq.sortBy(_._1).zipWithIndex.map(_.swap).toMap

    val rangeSplits = indexMapping.map {
      case (idx, (parentIdx, begin)) =>
        val end = indexMapping.get(idx + 1).flatMap {
          case (idx2, _) => partitionToFirstKey.get(idx2)
        }
        RangeSplit(OrderedRDDPartition(idx).asInstanceOf[Partition], CloseOpen(begin, end))
    }.toArray

    val indexToParentPartition = indexMapping.map {
      case (idx, (parentIdx, _)) => (idx, rdd.partitions(parentIdx))
    }

    new OrderedRDD[K, V](rdd.sparkContext, rangeSplits, Seq(new OneToOneDependency(rdd) {
      override def getParents(partitionId: Int) = List(indexMapping(partitionId)._1)
    }))(
      (partition, context) => rdd.iterator(indexToParentPartition(partition.index), context)
    )
  }

  /**
   * Convert an [[org.apache.spark.rdd.RDD]] to an [[OrderedRDD]]. Internally, it will sort the given rdd first and thus
   * will invoke shuffling of rows and thus will be slow and IO/Memory intensive.
   *
   * @param rdd The RDD of (K, SK, V) tuple(s) expected to convert.
   * @return an [[OrderedRDD]].
   */
  def fromUnsortedRDD[K: Ordering: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): OrderedRDD[K, V] = rdd match {
    case orderedRdd: OrderedRDD[K, V] => orderedRdd
    case _ => fromNormalizedSortedRDD(rdd.sortBy(_._1))
  }

  /**
   * Convert a sorted rows backed by a CSV file into an [[OrderedRDD]].
   *
   * @param sc The Spark context.
   * @param file The file path of CSV file.
   * @param numPartitions The number of partitions expects to split the resulted [[OrderedRDD]].
   * @return an [[OrderedRDD]].
   */
  def fromCSV[SK, V: ClassTag](
    sc: SparkContext,
    file: String,
    numPartitions: Int
  )(parse: Iterator[String] => Iterator[(Long, V)]): OrderedRDD[Long, V] = {
    // TODO: add unit test for it.
    val sortedRdd = fromInputFormat(
      sc, file, CSVInputFormatConf(TextInputFormatConf(file, numPartitions))
    ) {
      iter => parse(iter.map(_._2))
    }
    fromSortedRDD(sortedRdd)
  }

  /**
   * Note: the K parameter must provide a total ordering over the source data set. If not,
   * this will all fail in confusing ways.
   */
  private[this] def fromInputFormat[K1, V1, K: ClassTag, V](
    sc: SparkContext,
    file: String,
    ifConf: InputFormatConf[K1, V1]
  )(
    parse: Iterator[(ifConf.KExtract#Extracted, ifConf.VExtract#Extracted)] => Iterator[(K, V)]
  )(implicit ord: Ordering[K]): RDD[(K, V)] = {
    val fileSplits = Hadoop.fileSplits(sc, file, ifConf) {
      case r => parse(Iterator(r)).next._1
    }
    case class FilePartition(override val index: Int) extends Partition
    val broadcastFileSplits = sc.broadcast(fileSplits.map { case (idx, t) => (idx, t._2) })
    new RDD[(K, V)](sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] =
        parse(Hadoop.readRecords(ifConf)(broadcastFileSplits.value(split.index)))

      override protected def getPartitions: Array[Partition] =
        fileSplits.keys.map { idx => FilePartition(idx) }.toArray
    }
  }

  private[flint] def fromRDD[K: Ordering: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)],
    ranges: Seq[CloseOpen[K]]
  ): OrderedRDD[K, V] = {
    require(rdd.partitions.length == ranges.length)
    val splits = (rdd.partitions zip ranges).map {
      case (part, range) => RangeSplit(OrderedRDDPartition(part.index), range)
    }
    // Cannot call rdd.partitions on executors because getPartitions might access transient fields
    val parentParts = rdd.partitions
    new OrderedRDD[K, V](rdd.sparkContext, splits, Seq(new OneToOneDependency(rdd)))({
      case (part, context) => rdd.iterator(parentParts(part.index), context)
    })
  }

  /**
   * Constructing an [[OrderedRDD]] from a [[RDD]] with partition range information.
   * *
   * Let us assume that an [[OrderedRDD]] orderedRdd1 is constructed from an [[RDD]] rdd1 and an [[RDD]]
   * rdd2 is obtained by applying some  partition preserving operation on rdd1, i.e. rdd2 has the same partition
   * ranges as rdd1. To create an  [[OrderedRDD]] orderedRdd2 from rdd2,  one could simply reuse the partition
   * range information obtained from the construction of orderedRdd1 from rdd1 without recomputing it.
   *
   * @param rdd The [[RDD]] to create a new [[OrderedRDD]] from
   * @param deps dependencies of the [[OrderedRDD]] that is used to construct dependencies of the new [[OrderedRDD]].
   *             Should have only one dependency.
   * @param rangeSplits rangeSplits of the [[OrderedRDD]] that is used to construct rangeSplits of the new
   *                   [[OrderedRDD]].
   */
  private[flint] def fromRDD[K: Ordering: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)],
    deps: Seq[Dependency[_]],
    rangeSplits: Seq[RangeSplit[K]]
  ): OrderedRDD[K, V] = {
    require(deps.length == 1, "An OrderedRDD created from a RDD or a Dataframe should have only one dependency")
    val dep = deps.head

    require(
      dep.isInstanceOf[NarrowDependency[_]],
      s"Dependency must be narrow dependency. Dep: ${dep}"
    )

    val narrowDep = dep.asInstanceOf[NarrowDependency[_]]

    val indexToParentPartIndices = TreeMap(rangeSplits.map {
      case RangeSplit(part, _) => (part.index, narrowDep.getParents(part.index))
    }: _*)

    // map(identify) because of https://issues.scala-lang.org/browse/SI-7005
    val indexToParentParts = indexToParentPartIndices.mapValues {
      case indices: Seq[Int] => indices.map(rdd.partitions(_))
    }.map(identity)

    // It is cleaner to create the dependency map instead of put the old dependencies object inside the new one.
    val newDep = narrowDep match {
      case d: OneToOneDependency[_] => new OneToOneDependency(rdd)
      case d: NarrowDependency[_] =>
        new NarrowDependency(rdd) {
          override def getParents(partitionId: Int): Seq[Int] =
            indexToParentPartIndices(partitionId)
        }
      case _ => sys.error(s"Unsupported dependency ${narrowDep}")
    }

    // This copy might not be necessary because OrderedRDDPartition is immutable. However, the code is cleaner this way
    // because we always create new partitions for a new rdd, instead of passing the partitions of an existing rdd to a
    // new rdd.
    val newRangeSplits = rangeSplits.map(_.copy[K]())

    new OrderedRDD[K, V](rdd.sparkContext, newRangeSplits, Seq(newDep))({
      case (part, context) =>
        val index = part.index
        val range = rangeSplits(index).range
        val parentParts = indexToParentParts(index)
        OrderedIterator(
          PartitionsIterator(rdd, parentParts, context)
        ).filterByRange(range)
    })
  }

}
