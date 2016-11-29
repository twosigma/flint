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

package com.twosigma.flint.timeseries

import com.twosigma.flint.rdd.OrderedRDD
import com.twosigma.flint.timeseries.row.{ InternalRowUtils, Schema }
import com.twosigma.flint.{ SharedSparkContext, SpecUtils }
import org.apache.spark.sql.CatalystTypeConvertersWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DoubleType, IntegerType, LongType, StructType }
import org.scalatest.FlatSpec

import scala.collection.mutable

object TestHelper {
  private val schema = Schema("tid" -> IntegerType, "price" -> DoubleType)
  val converter: InternalRow => GenericRowWithSchema = CatalystTypeConvertersWrapper.toScalaRowConverter(schema)

  def filter(ts: Long, row: InternalRow): Boolean = {
    row.getInt(1) > 5
  }

  def copy(tuple: (Long, InternalRow)): (Long, InternalRow) = {
    (tuple._1, tuple._2.copy)
  }

  def shift(ts: Long): Long = ts - 100

  def map(tuple: (Long, InternalRow)): (Long, InternalRow) = {
    val newRow = InternalRowUtils.update(tuple._2, schema, (1, tuple._1.toInt))
    (tuple._1, newRow)
  }
}

class UnsafeOrderedRDDSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/leftjoin"

  private def from(filename: String, schema: StructType): TimeSeriesRDD =
    SpecUtils.withResource(s"$resourceDir/$filename") { source =>
      CSV.from(
        sqlContext,
        s"file://$source",
        header = true,
        sorted = true,
        schema = schema
      ).repartition(defaultPartitionParallelism)
    }

  private def compareRowReferences(rdd: OrderedRDD[Long, InternalRow]) = {
    val cmp = rdd.mapPartitionsWithIndexOrdered {
      case (_, iter) =>
        val rowCopies = mutable.ListBuffer.empty[(Long, InternalRow)]
        val rowReferences = mutable.ListBuffer.empty[(Long, InternalRow)]
        iter.foreach {
          case (ts, row) =>
            val tuple = (ts, row)
            rowReferences += tuple
            val tupleCopy = (ts, row.copy())
            rowCopies += tupleCopy
        }

        val results = rowCopies.zip(rowReferences).map {
          case (copy, reference) => (1L, copy.equals(reference))
        }

        results.iterator
    }.collect()

    cmp.map(_._2)
  }

  private def usesOneRowBufferPerPartition(rdd: OrderedRDD[Long, InternalRow]) = {
    val cmp = rdd.mapPartitionsWithIndexOrdered {
      case (_, iter) =>
        val rowReferences = mutable.ListBuffer.empty[InternalRow]
        iter.foreach {
          case (ts, row) =>
            rowReferences += row
        }

        val firstReference = rowReferences.head
        val allEqual = rowReferences.tail.forall(anotherRowReference => anotherRowReference == firstReference)

        Seq((1L, allEqual)).toIterator
    }.collect()

    cmp.map(_._2)
  }

  "UnsafeOrderedRDDSpec" should "allow referencing timeseriesRdd.orderedRdd rows" in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true)
      val impl = timeseriesRdd.asInstanceOf[TimeSeriesRDDImpl]

      val safeCmp = compareRowReferences(impl.orderedRdd)
      assert(safeCmp.forall(cmp => cmp))

      val map = compareRowReferences(impl.unsafeOrderedRdd)
      assert(!map.forall(cmp => cmp))
    }
  }

  it should "return correct data frames" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = from(
      "JoinOnTime.results",
      Schema("tid" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )
    val expectedRows = resultsTSRdd.collect()

    val priceDf = priceTSRdd.toDF
    val volumeDf = volumeTSRdd.toDF
    val joinedDf = priceDf.join(volumeDf, Seq("time", "tid"))

    val joinedRows = joinedDf.collect()
    // the order isn't guaranteed, but DF join() should return the same set
    assert(expectedRows.toSet == joinedRows.toSet)
  }

  it should "support multiple iterations" in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true)
      val impl = timeseriesRdd.asInstanceOf[TimeSeriesRDDImpl]

      // explicitly making a copy to catch issues even if orderedRdd is broken
      val safeCopy = impl.orderedRdd.mapOrdered(TestHelper.copy)

      val unsafeFiltered = impl.unsafeOrderedRdd.filterOrdered(TestHelper.filter)
      val safeFiltered = safeCopy.filterOrdered(TestHelper.filter)
      // collect() can't be used with UnsafeRow
      assert(unsafeFiltered.mapOrdered(TestHelper.copy).collect().deep == safeFiltered.collect().deep)

      val unsafeShifted = impl.unsafeOrderedRdd.shift(TestHelper.shift)
      val safeShifted = safeCopy.shift(TestHelper.shift)
      assert(unsafeShifted.mapOrdered(TestHelper.copy).collect().deep == safeShifted.collect().deep)

      val unsafeMappeed = impl.unsafeOrderedRdd.mapOrdered(TestHelper.map)
      val safeMappeed = safeCopy.mapOrdered(TestHelper.map)
      assert(unsafeMappeed.mapOrdered(TestHelper.copy).collect().deep == safeMappeed.collect().deep)
    }
  }

  it should "collect rows correctly" in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true)
      val impl = timeseriesRdd.asInstanceOf[TimeSeriesRDDImpl]

      val expectedRows = timeseriesRdd.toDF.collect()
      val internalRows = impl.orderedRdd.collect().map(_._2).map(TestHelper.converter)
      assert(internalRows.toSet == expectedRows.toSet)

      val originalInternalRows = impl.unsafeOrderedRdd.collect().map(_._2).map(TestHelper.converter)
      assert(originalInternalRows.toSet != expectedRows.toSet)
    }
  }

  it should "reuse row buffer object in unsafeOrderedRdd.mapPartitionsWithIndexOrdered" in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true)
      val impl = timeseriesRdd.asInstanceOf[TimeSeriesRDDImpl]

      val safeCmp = usesOneRowBufferPerPartition(impl.orderedRdd)
      assert(safeCmp.forall(cmp => !cmp))

      val unsafeCmp = usesOneRowBufferPerPartition(impl.unsafeOrderedRdd)
      assert(unsafeCmp.forall(cmp => cmp))
    }
  }
}
