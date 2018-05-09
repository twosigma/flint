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

import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType, LongType }

import scala.collection.mutable

class TimeSeriesGeneratorSpec extends TimeSeriesSuite {
  private val begin = 1000000L
  private val end = 2000000L
  private val frequency = 10000L
  private val schema = Schema(
    "time" -> LongType,
    "id" -> IntegerType,
    "x1" -> DoubleType,
    "x2" -> DoubleType
  )
  private val seed = 31415926L

  private val nextDouble = (_: Long, _: Int, r: util.Random) => r.nextDouble()

  "TimeSeriesGenerator" should "generate TimeSeriesRDD randomly " in {
    val randomTSRdd = new TimeSeriesGenerator(sc, begin, end, frequency)(
      columns = Seq(
        "x1" -> nextDouble,
        "x2" -> nextDouble
      ),
      seed = seed
    ).generate()

    assert(randomTSRdd.schema == schema)
    assert(randomTSRdd.count() == (end - begin) / frequency + 1)
  }

  it should "generate TimeSeriesRDD randomly with fixed ids set for different cycles" in {
    val randomTSRdd = new TimeSeriesGenerator(sc, begin, end, frequency)(
      columns = Seq(
        "x1" -> nextDouble,
        "x2" -> nextDouble
      ),
      seed = seed,
      ids = 1 to 5
    ).generate()

    assert(randomTSRdd.schema == schema)
    assert(randomTSRdd.groupByCycle().count() == (end - begin) / frequency + 1)
    randomTSRdd.groupByCycle().collect().foreach{
      r =>
        val ids = r.getAs[mutable.WrappedArray[Row]]("rows").map {
          r => r.getAs[Int]("id")
        }.sorted
        assert(ids.size == 5)
        assert(ids.deep == (1 to 5).toArray.deep)
    }
  }

  it should "generate TimeSeriesRDD randomly with random ids set for different cycles" in {
    (1 to 10).foreach {
      i =>
        val randomTSRdd = new TimeSeriesGenerator(sc, begin, end, frequency)(
          columns = Seq(
            "x1" -> nextDouble,
            "x2" -> nextDouble
          ),
          seed = seed + i,
          ids = 1 to 5,
          ratioOfCycleSize = 0.5
        ).generate()

        assert(randomTSRdd.schema == schema)
        assert(randomTSRdd.groupByCycle().count() == (end - begin) / frequency + 1)

        val setsOfIds = randomTSRdd.groupByCycle().collect().map {
          r =>
            val currentIds = r.getAs[mutable.WrappedArray[Row]]("rows").map { r => r.getAs[Int]("id") }.sorted
            assert(currentIds.size == 3)
            currentIds
        }.toSet
        assert(setsOfIds.size <= 10)
        assert(setsOfIds.size > 5)
    }
  }

  it should "generate TimeSeriesRDD randomly with uneven intervals" in {
    (1 to 10).foreach {
      i =>
        val randomTSRdd = new TimeSeriesGenerator(sc, begin, end, frequency)(
          columns = Seq(
            "x1" -> nextDouble,
            "x2" -> nextDouble
          ),
          seed = seed + i,
          uniform = false
        ).generate()

        assert(randomTSRdd.schema == schema)
        assert(randomTSRdd.groupByCycle().count() > 1.5 * (end - begin) / frequency)
        assert(randomTSRdd.groupByCycle().count() < 2.5 * (end - begin) / frequency)
    }
  }
}
