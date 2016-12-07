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

import com.twosigma.flint.rdd.function.group.{ GroupByKeyIterator, Intervalize }
import com.twosigma.flint.rdd.function.join._
import com.twosigma.flint.rdd.function.summarize._
import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.OverlappableSummarizer
import com.twosigma.flint.annotation.PythonApi
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{ RDD, ShuffledRDD }
import org.apache.spark._
import scala.reflect.ClassTag

private[rdd] case class OrderedRDDPartition(override val index: Int) extends Partition

object OrderedRDD {

  /**
   * Convert an [[org.apache.spark.rdd.RDD RDD]] to an [[OrderedRDD]].
   *
   * @param rdd     The [[org.apache.spark.rdd.RDD RDD]] of (K, V) tuples expected to convert.
   * @param rddType The type of `rdd`.
   * @param keyRdd  The [[org.apache.spark.rdd.RDD]] of keys of `rdd`. The `keyRdd` is expected to have exactly the
   *                same distribution of keys as that of `rdd`, i.e. same number of partition; the i-th partitions
   *                of `rdd` and `keyRdd` have exactly the same sequences of keys. An example of `keyRdd` could
   *                be obtained by {{ rdd.map(_._1) }}. This parameter is optional with default null.
   * @return an [[OrderedRDD]].
   */
  def fromRDD[K: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)],
    rddType: KeyPartitioningType,
    keyRdd: RDD[K] = null
  )(implicit ord: Ordering[K]): OrderedRDD[K, V] = rddType match {
    case KeyPartitioningType.NormalizedSorted => Conversion.fromNormalizedSortedRDD(rdd)
    case KeyPartitioningType.Sorted => Conversion.fromSortedRDD(rdd, keyRdd)
    case KeyPartitioningType.UnSorted => Conversion.fromUnsortedRDD(rdd)
  }

  /**
   * Convert a sorted [[org.apache.spark.rdd.RDD RDD]] to an [[OrderedRDD]] with no extra cost.
   * This function takes an extra parameter `ranges` to specify partition boundaries.
   *
   * @param ranges The partition boundaries which must be close-open for all partitions.
   * @note Unless you know what you are doing, do not use this function.
   */
  @DeveloperApi
  @PythonApi
  def fromRDD[K: Ordering: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)],
    ranges: Seq[CloseOpen[K]]
  ): OrderedRDD[K, V] = Conversion.fromRDD(rdd, ranges)

  @DeveloperApi
  @PythonApi
  def fromRDD[K: Ordering: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)],
    deps: Seq[Dependency[_]],
    rangeSplits: Seq[RangeSplit[K]]
  ): OrderedRDD[K, V] = Conversion.fromRDD(rdd, deps, rangeSplits)

  /**
   * Convert a sorted rows stored as a CSV file into an [[OrderedRDD]].
   *
   * @param sc            The Spark context.
   * @param file          The file path of CSV file.
   * @param numPartitions The number of partitions expects to split the returning [[OrderedRDD]].
   * @return an [[OrderedRDD]].
   */
  def fromCSV[V: ClassTag](
    sc: SparkContext,
    file: String,
    numPartitions: Int
  )(parse: Iterator[String] => Iterator[(Long, V)]): OrderedRDD[Long, V] =
    Conversion.fromCSV(sc, file, numPartitions)(parse)
}

/**
 * An RDD extension that represents an ordered dataset.
 *
 * The rows of an [[OrderedRDD]] are key-value tuples (K, V) and they have been sorted by the ordering of K.
 * It associates a [[CloseOpen]] range [b, e) to each partition such that all keys of rows in the partition
 * are bounded by it.
 *
 * The following are the invariants of [[OrderedRDD]]:
 *   - partition range is close-open range [b, e), where b is the first key and e might NOT be the first key
 *     of the next partition;
 *   - a key cannot appear in more than one partition;
 *   - a partition must be non-empty.
 */
class OrderedRDD[K: ClassTag, V: ClassTag](
  @transient val sc: SparkContext,
  @transient private[flint] val splits: Seq[RangeSplit[K]],
  @transient private[flint] val deps: Seq[Dependency[_]] = Nil
)(create: (Partition, TaskContext) => Iterator[(K, V)])(implicit ord: Ordering[K])
  extends RDD[(K, V)](sc, deps) {

  private val self = this

  /**
   * A sequence of [[RangeSplit]]s sorted by their partitions' indices where a [[RangeSplit]]
   * represents an non-empty partition with its [[Range]] information.
   */
  val rangeSplits: Array[RangeSplit[K]] = splits.sortBy(_.partition.index).toArray

  // Sanity checks
  if (rangeSplits.length > 1) {
    require(
      rangeSplits.map(_.range.end).dropRight(1).forall(_.isDefined),
      "Only the final partition is allowed not to have a bound"
    )

    // Check whether they are increasing and non overlapping.
    rangeSplits.headOption.map {
      s0 =>
        rangeSplits.foldLeft((s0, true)) {
          case ((s1, isOrdered), s2) =>
            s1.range.end.fold(sys.error("Only the final partition is allowed to be unbound.")) {
              e => (s2, isOrdered && ord.lteq(e, s2.range.begin))
            }
        }
    }

    rangeSplits.zipWithIndex.foreach {
      case (split, idx) => require(
        split.partition.index == idx,
        "The indices of rangeSplits are not consistent to their partitions indices."
      )
    }

    deps.foreach {
      case n: NarrowDependency[_] =>
        for (partIndex <- rangeSplits.indices) {
          for (parentPart <- n.getParents(partIndex)) {
            require(
              parentPart < n.rdd.getNumPartitions,
              s"Partition $partIndex depends on partition number $parentPart, but it doesn't exist in the parent RDD."
            )
          }
        }

      case _ =>
    }
  }

  // ==============================================================================================
  // Internal or developer APIs
  // ==============================================================================================

  override def compute(partition: Partition, context: TaskContext): Iterator[(K, V)] = create(partition, context)

  override protected def getPartitions: Array[Partition] = rangeSplits.map(_.partition)

  /**
   * @note We can totally use the info that we get from the splits, but right now there is no point
   * because we're not running on the HDFS nodes anyway.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /**
   * Similar to [[org.apache.spark.rdd.RDD.mapPartitionsWithIndex]], it returns an [[OrderedRDD]] by applying a function
   * to each partition of this [[OrderedRDD]], while tracking the index of the original partition.
   *
   * @note key must not be changed.
   */
  @DeveloperApi
  def mapPartitionsWithIndexOrdered[V2: ClassTag](
    f: (Int, Iterator[(K, V)]) => Iterator[(K, V2)]
  ): OrderedRDD[K, V2] = {
    new OrderedRDD(self.sc, rangeSplits, Seq(new OneToOneDependency(self)))(
      (partition, taskContext) => f(partition.index, self.iterator(partition, taskContext))
    )
  }

  // ==============================================================================================
  // Public APIs
  // ==============================================================================================

  /**
   * Return the partition ranges for this [[OrderedRDD]] as a sequence.
   */
  val getPartitionRanges: Seq[CloseOpen[K]] = rangeSplits.map(_.range)

  /**
   * Return a new [[OrderedRDD]] that has exactly `numPartitions` partitions.
   *
   * Can increase or decrease the level of parallelism in this [[OrderedRDD]]. Internally, this uses
   * a shuffle to redistribute data if it tries to increase the numPartitions.
   *
   * If you are decreasing the number of partitions in this [[OrderedRDD]], consider using
   * [[OrderedRDD]]'s coalesce method, which can avoid performing a shuffle.
   *
   * @param numPartitions The expected number of partitions.
   * @return a new [[OrderedRDD]] that has exactly `numPartitions` partitions.
   */
  def repartition(numPartitions: Int): OrderedRDD[K, V] = {
    val partitioner = new RangePartitioner(numPartitions, self, true)
    // TODO: we should improve the Partitioner or the ShuffledRDD to report the Range per partition
    //       which we should use later for fromSortedRDD to avoid extra read and thus lazy.
    Conversion.fromSortedRDD(new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ord))
  }

  /**
   * Return a new [[OrderedRDD]] that is reduced to numPartitions partitions.
   *
   * @param numPartitions The number of target partitions which must be positive and less then the number of
   *                      partitions of this [[OrderedRDD]].
   * @return a new [[OrderedRDD]] with partitions.length == numPartitions
   */
  def coalesce(numPartitions: Int): OrderedRDD[K, V] = {
    require(
      numPartitions > 0 && numPartitions <= rangeSplits.length,
      s"Number of partitions must be within 1 and ${rangeSplits.length}"
    )

    // table of ranges and partitions from coalescing
    val coalesced = (0 until numPartitions).map {
      i =>
        // array indices of aggregated partitions
        val begin = ((i.toLong * rangeSplits.length) / numPartitions).toInt
        val end = (((i.toLong + 1) * rangeSplits.length) / numPartitions).toInt

        // ranges and partitions that will be coalesced together
        val mySplits = rangeSplits.slice(begin, end)
        val coalescedRanges = CloseOpen(
          mySplits(0).range.begin,
          mySplits(mySplits.length - 1).range.end
        )
        val coalescedPartitions = mySplits.map(_.partition)
        (i, coalescedRanges, coalescedPartitions)
    }

    // array of RangeSplits with new partition index
    val coalescedSplits = coalesced.map{
      x => RangeSplit(OrderedRDDPartition(x._1).asInstanceOf[Partition], x._2)
    }

    // dependencies to parent RDD
    val parents = coalesced.map(x => x._1 -> x._3).toMap
    val dep = new NarrowDependency(this) {
      override def getParents(partitionId: Int) = parents(partitionId).map(_.index)
    }

    // return new OrderedRDD
    new OrderedRDD[K, V](sc, coalescedSplits, Seq(dep))({
      case (partition, context) =>
        PartitionsIterator(self, parents(partition.index), context)
    })
  }

  /**
   * Merge this [[OrderedRDD]] with another [[OrderedRDD]] while preserving the ordering of key.
   *
   * See [[com.twosigma.flint.rdd.function.join.Merge\$.apply]] for more information.
   *
   * @param that The other [[OrderedRDD]] expected to merge with.
   * @return the merged [[OrderedRDD]].
   */
  // TODO: check the invariants of the merged OrderedRDD.
  def merge(that: OrderedRDD[K, V]): OrderedRDD[K, V] = Merge(self, that)

  /**
   * Merge this [[OrderedRDD]] with another [[OrderedRDD]] while preserving the ordering of key.
   *
   * The other [[OrderedRDD]] could have different value type. See [[com.twosigma.flint.rdd.function.join.Merge\$.++]]
   * for more information.
   *
   * @param that The other [[OrderedRDD]] expected to merge with.
   * @return the merged [[OrderedRDD]].
   */
  // TODO: check the invariants of the merged OrderedRDD.
  def ++[V2: ClassTag](that: OrderedRDD[K, V2]): OrderedRDD[K, Either[V, V2]] = Merge.++(self, that)

  /**
   * Left-join two tables as of a key and or on a secondary key.
   *
   * See [[com.twosigma.flint.rdd.function.join.LeftJoin\$.apply]] for more information.
   *
   * @param that        Right [[OrderedRDD]] to join; the left one is simply this object.
   * @param toleranceFn Function that provides the maximum allowed tolerance to lookback.
   * @param leftSk      Function that returns the desired secondary key from the left.
   * @param rightSk     Function that returns the desired secondary key from the right.
   * @note consider allowing more than one right at the time. It should avoid an extra scan on the
   *       left. Need to muse on how spark will schedule that, because it may pipeline straight into the
   *       next stage and be good enough.
   */
  def leftJoin[SK1, SK2, V2: ClassTag](
    that: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK1,
    rightSk: V2 => SK2
  )(implicit ev: SK1 =:= SK2): OrderedRDD[K, (V, Option[(K, V2)])] = LeftJoin(self, that, toleranceFn, leftSk, rightSk)

  /**
   * Left-join this and the other [[OrderedRDD]] as of a key and or on a secondary key using inexact key
   * matches. For each row in the left, append the first row from the right at or
   * after the same time. Similar to [[OrderedRDD.leftJoin]] except that it joins with future rows when no matching
   * key are found.
   *
   * @param that          Right [[OrderedRDD]] to join; the left is simply this object.
   * @param toleranceFn   Function that provides the maximum allowed tolerance to lookahead.
   * @param leftSk        Function that returns the desired secondary key from the left table.
   * @param rightSk       Function that returns the desired secondary key from the right table.
   * @param strictForward A flag specifies when performing a future left join, join rows where keys
   *                      exactly match. The default is false.
   * @note consider allowing more than one right at the time. It should avoid an extra scan on the
   *       left. Need to muse on how spark will schedule that, because it may pipeline straight into the
   *       next stage and be good enough.
   */
  def futureLeftJoin[SK1, SK2, V2: ClassTag](
    that: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK1,
    rightSk: V2 => SK2,
    strictForward: Boolean = false
  )(implicit ev: SK1 =:= SK2): OrderedRDD[K, (V, Option[(K, V2)])] =
    FutureLeftJoin(self, that, toleranceFn, leftSk, rightSk, strictForward)

  /**
   * Outer-join two tables as of a timestamp and on a secondary key.
   *
   * @param that        Right table to join; the left table is simply this object
   * @param toleranceFn Function that provides the maximum allowed time to lookback
   * @param leftSk      Function that returns the desired secondary key from the left table
   * @param rightSk     Function that returns the desired secondary key from the right table
   */
  def symmetricJoin[SK, V2: ClassTag](
    that: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK
  ): OrderedRDD[K, (Option[(K, V)], Option[(K, V2)])] = SymmetricJoin(self, that, toleranceFn, leftSk, rightSk)

  /**
   * For each row of an [[OrderedRDD]], apply a [[Summarizer]] to all rows of its window.
   *
   * @param window     A function defines the window for a given key.
   * @param summarizer A [[Summarizer]] expects to apply.
   * @return a [[OrderedRDD]] with windowing summaries.
   */
  def summarizeWindows[SK, V1, U, V2: ClassTag](
    window: K => (K, K),
    summarizer: Summarizer[V, U, V2],
    sk: V => SK
  ): OrderedRDD[K, (V, V2)] = SummarizeWindows(self, window, summarizer, sk)

  /**
   * Apply a [[Summarizer]] to all rows of an [[OrderedRDD]].
   *
   * @param summarizer A [[Summarizer]] expects to apply.
   * @param skFn       A function to extract keys from rows. The summarization will be applied per key level.
   * @return the summarized result(s).
   */
  def summarize[SK, U, V2](summarizer: Summarizer[V, U, V2], skFn: V => SK): Map[SK, V2] =
    Summarize(self, summarizer, skFn)

  /**
   * Apply an [[OverlappableSummarizer]] to all rows of an [[OrderedRDD]].
   *
   * @param summarizer A [[OverlappableSummarizer]] expects to apply.
   * @param windowFn   A function that defines how much it needs to overlap, i.e. for a particular row, how much it
   *                   needs to look-back or look-forward. Internally, a partition will be expanded by the windows of
   *                   its first row and last row.
   * @param skFn       A function to extract keys from rows. The summarization will be applied per key level.
   * @return the summarized result(s).
   */
  def summarize[SK, U, V2](
    summarizer: OverlappableSummarizer[V, U, V2],
    windowFn: K => (K, K),
    skFn: V => SK
  ): Map[SK, V2] = Summarize(self, summarizer, windowFn, skFn)

  /**
   * Similar to [[org.apache.spark.rdd.RDD.zipWithIndex]], it zips values of this [[OrderedRDD]] with
   * its element indices. The ordering of this [[OrderedRDD]] will be preserved.
   *
   * @return a new [[OrderedRDD]] by zipping its element indices to all values of this [[OrderedRDD]].
   */
  def zipWithIndexOrdered: OrderedRDD[K, (V, Long)] = {
    val withIndex = this.zipWithIndex
    val indexToPartition = sc.broadcast(withIndex.partitions.map { p => (p.index, p) }.toMap)
    new OrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(withIndex)))(
      (p, tc) => withIndex.iterator(indexToPartition.value(p.index), tc).map {
        case ((k, v), idx) => (k, (v, idx))
      }
    )
  }

  /**
   * Applies a function to all rows and sorts the result RDD. Use the [[OrderedRDD.mapValues]]
   * transformation if you don't need to update keys.
   *
   * @param fn A function that takes and returns a key-value pair.
   * @return a new [[OrderedRDD]] by applying a function to all rows and sorting the result by key.
   */
  def mapOrdered[V2: ClassTag](fn: ((K, V)) => (K, V2)): OrderedRDD[K, V2] =
    OrderedRDD.fromRDD(super.map(fn), KeyPartitioningType.UnSorted)

  /**
   * Similar to [[org.apache.spark.rdd.RDD.map]], but the ordering of this [[OrderedRDD]] will be preserved.
   *
   * @return a new [[OrderedRDD]] by applying a function to all values of this [[OrderedRDD]].
   */
  def mapValues[V2: ClassTag](fn: (K, V) => V2): OrderedRDD[K, V2] =
    new OrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(this)))(
      (p, tc) => iterator(p, tc).map { case (k, v) => (k, fn(k, v)) }
    )

  /**
   * Similar to [[org.apache.spark.rdd.RDD.flatMap]], but the ordering of this [[OrderedRDD]] will be preserved.
   *
   * @return a new [[OrderedRDD]] by applying a function to all values of this [[OrderedRDD]] and then
   *         flattening the results.
   */
  def flatMapValues[V2: ClassTag](fn: (K, V) => TraversableOnce[V2]): OrderedRDD[K, V2] =
    new OrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(this)))(
      (p, tc) => iterator(p, tc).flatMap { case (k, v) => fn(k, v).map((k, _)) }
    )

  /**
   * Similar to [[org.apache.spark.rdd.RDD.filter]], but the ordering of this [[OrderedRDD]] will be preserved.
   *
   * @return a new [[OrderedRDD]] containing only the elements that satisfy a predicate.
   */
  def filterOrdered(fn: (K, V) => Boolean): OrderedRDD[K, V] =
    new OrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(this)))(
      (p, tc) => iterator(p, tc).filter { case (k, v) => fn(k, v) }
    )

  /**
   * Similar to [[org.apache.spark.rdd.RDD]]'s collect, but the ordering of this [[OrderedRDD]] will be preserved.
   */
  def collectOrdered[V2: ClassTag](fn: PartialFunction[(K, V), V2]): OrderedRDD[K, V2] =
    new OrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(this)))(
      (p, tc) =>
        iterator(p, tc).collect(new PartialFunction[(K, V), (K, V2)] {
          override def apply(d: (K, V)) = (d._1, fn(d._1, d._2))

          override def isDefinedAt(d: (K, V)) = fn.isDefinedAt(d)
        })
    )

  /**
   * Group by the primary keys.
   *
   * If a function mapping from rows to their secondary keys is provided, it will return an [[OrderedRDD]]
   * whose rows are further partitioned into subgroups by their secondary keys.
   *
   * @param skFn A function that extracts a secondary key from a row.
   * @return an [[OrderedRDD]] of grouped rows with the same key. The ordering of rows in each group is preserved.
   */
  def groupByKey[SK](skFn: V => SK): OrderedRDD[K, Array[V]] = {
    new OrderedRDD[K, Array[V]](sc, rangeSplits, Seq(new OneToOneDependency(this)))(
      (p, tc) => GroupByKeyIterator(iterator(p, tc), skFn)
    )
  }

  /**
   * Intervalize this [[OrderedRDD]] by mapping its keys to the begin or the end of an interval
   * where they fall into. The intervals are defined by the given intervalizer.
   *
   * @param intervalizer   A sequence of sorted keys where two sequential keys are treated as an interval.
   * @param beginInclusive A flag to determine how to treat keys that fall exactly on the
   *                       ticks. If it is true, keys that are at the exact beginning of an interval will be
   *                       included and keys that fall on the exact end will be excluded, as represented by
   *                       the interval [begin, end). Otherwise, it is (begin, end].
   * @return An [[OrderedRDD]] whose keys are intervalized and the original keys are kept in the
   *         values as (K, V)s.
   */
  def intervalize(intervalizer: IndexedSeq[K], beginInclusive: Boolean): OrderedRDD[K, (K, V)] =
    Intervalize.intervalize(self, intervalizer, beginInclusive)

  /**
   * Intervalize an this [[OrderedRDD]] by mapping its keys to the begin or the end of an interval
   * where they fall into. The intervals are defined by the given intervalizer.
   *
   * @param intervalizer   An [[org.apache.spark.rdd.RDD RDD]] of sorted keys where two sequential keys are
   *                       treated as an interval.
   * @param beginInclusive A flag to determine how to treat keys that fall exactly on the
   *                       ticks. If it is true, keys that are at the exact beginning of an interval will be
   *                       included and keys that fall on the exact end will be excluded, as represented by
   *                       the interval [begin, end). Otherwise, it is (begin, end].
   * @return an [[OrderedRDD]] whose keys are intervalized and the original keys are kept in the values as (K, V)s.
   */
  def intervalize[V1](intervalizer: OrderedRDD[K, V1], beginInclusive: Boolean): OrderedRDD[K, (K, V)] =
    Intervalize.intervalize(self, intervalizer, beginInclusive)

  /**
   * See doc [[com.twosigma.flint.rdd.function.summarize.Summarizations.apply]] for more information.
   *
   * @return an [[OrderedRDD]] of tuples (K, (V, V2)) where V2 is the summary of all rows prior
   * to and also including the current row.
   */
  def summarizations[SK, U, V2](
    summarizer: Summarizer[V, U, V2],
    sk: V => SK
  ): OrderedRDD[K, (V, V2)] = Summarizations(self, summarizer, sk)

  /**
   * Change the key for each row.
   *
   * @param fn Function that monotonically shifts keys in one direction.
   * @return an [[OrderedRDD]] whose row keys have been shifted monotonically to one direction.
   * @note It may break the invariants of OrderedRDD, i.e. no key across multiple partitions.
   */
  // TODO: check the invariants of the merged OrderedRDD.
  def shift(fn: K => K): OrderedRDD[K, V] = {
    val newRangeSplits = rangeSplits.map { rs => RangeSplit(rs.partition, rs.range.shift(fn)) }
    new OrderedRDD(sc, newRangeSplits, Seq(new OneToOneDependency(self)))(
      (p, tc) => iterator(p, tc).map { case (k, v) => (fn(k), v) }
    )
  }

}
