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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.clock.{ RandomClock, UniformClock }
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DFConverter, SparkSession }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.util
import scala.util.Random

/**
 * A generator to generate a random [[TimeSeriesRDD]].
 *
 * For example, the following piece of code will generate a [[TimeSeriesRDD]] such that
 *   - schema is ["time", "id", "x1", "x2"];
 *   - timestamps of cycles are uniformly distributed;
 *   - each cycle has 5 rows whose ids are from 1 to 5;
 *   - user specifies how random values under `x1` and `x2` are generated.
 *
 * {{{
 *   new TimeSeriesGenerator(sc, begin = 1000L, end = 2000L, frequency = 100L)(
 *     columns = Seq(
 *       "x1" -> { (t: Long, id: Int, r: util.Random) => r.nextDouble() },
 *       "x2" -> { (t: Long, id: Int, r: util.Random) => r.nextDouble() }
 *     ),
 *     ids = (1 to 5)
 *   ).generate()
 * }}}
 *
 *
 * @param begin            The inclusive begin of time range for the [[TimeSeriesRDD]].
 * @param end              The inclusive end of time range for the [[TimeSeriesRDD]].
 * @param frequency        The length of intervals between cycles.
 * @param uniform          Whether cycles are uniformly distributed. If true, intervals between sequential cycles
 *                         are fixed length, i.e. `frequency`. If false, intervals between sequential cycles
 *                         have various lengths whose values are uniformly distributed in the range of
 *                         [1, `frequency`].
 * @param ids              The set of possible integer ids per cycle. Default is {{{ Seq(1) }}}.
 * @param ratioOfCycleSize The percentage of number of distinct ids that appear in each cycle.
 *                         E.g. if the ratio is 0.5, the number of distinct randomly selected ids per cycle
 *                         is 0.5 * |`ids`|. If the ratio is 1.0, then every cycle has the same set of ids.
 *                         Default 1.0.
 * @param columns          A sequence of tuples each of which specifies the name of an extra column and a function
 *                         to generate a random value for a given timestamp and id. Default empty sequence.
 * @param numSlices        The number of desired partitions of the [[TimeSeriesRDD]]. Default
 *                         {{{ sc.defaultParallelism }}}.
 * @param seed             The random seed expected to use. Default current time in milliseconds.
 */
class TimeSeriesGenerator(
  @transient val sc: SparkContext,
  begin: Long,
  end: Long,
  frequency: Long
)(
  uniform: Boolean = true,
  ids: Seq[Int] = Seq(1),
  ratioOfCycleSize: Double = 1.0,
  columns: Seq[(String, (Long, Int, util.Random) => Double)] = Seq.empty,
  numSlices: Int = sc.defaultParallelism,
  seed: Long = System.currentTimeMillis()
) extends Serializable {
  require(ids.nonEmpty, s"ids must be non-empty.")

  private val schema = {
    var _schema = Schema(
      "time" -> LongType,
      "id" -> IntegerType
    )
    columns.foreach {
      case (columnName, _) => _schema = _schema.add(columnName, DoubleType)
    }
    _schema
  }

  def generate(): TimeSeriesRDD = {
    val cycles = if (uniform) {
      new UniformClock(sc, begin = begin, end = end, frequency = frequency, offset = 0L, endInclusive = true)
    } else {
      new RandomClock(sc, begin = begin, end = end, frequency = frequency, offset = 0L,
        seed = seed, endInclusive = true)
    }
    val cycleSize = math.max(math.ceil(ids.size * ratioOfCycleSize), 1).toInt

    val orderedRdd = cycles.asOrderedRDD(numSlices).mapPartitionsWithIndexOrdered {
      case (partIndex, iter) =>
        val rand = new Random(seed + partIndex)
        def getCycle(time: Long): Seq[InternalRow] = {
          val randIds = rand.shuffle(ids).take(cycleSize)
          randIds.map {
            id =>
              val values = columns.map {
                case (_, fn) =>
                  fn(time, id, rand)
              }
              InternalRow.fromSeq(time +: id +: values)
          }
        }
        iter.map(_._2).flatMap{ case t => getCycle(t).map((t, _)) }
    }

    val df = DFConverter.toDataFrame(orderedRdd, schema)
    TimeSeriesRDD.fromDFWithRanges(df, orderedRdd.getPartitionRanges.toArray)
  }
}
