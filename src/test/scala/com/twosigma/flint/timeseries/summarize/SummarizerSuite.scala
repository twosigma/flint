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

package com.twosigma.flint.timeseries.summarize

import com.twosigma.flint.timeseries._
import org.apache.commons.math3.primes
import org.apache.spark.sql.CatalystTypeConvertersWrapper
import org.apache.spark.sql.functions.col

sealed trait SummarizerProperty {

  def test(
    timeSeriesRdd: TimeSeriesRDD,
    summarizerFactory: SummarizerFactory
  ): Unit
}

class SummarizerSuite extends TimeSeriesSuite {

  // Use the smallest prime number that is larger than 32 as default parallelism.
  override val defaultPartitionParallelism: Int = primes.Primes.nextPrime(32)

  private val cycles = 10000L

  private val frequency = 100L

  lazy val AllData = Seq(
    new TimeSeriesGenerator(
      sc,
      begin = 0L,
      end = cycles * frequency,
      frequency = frequency
    )(
      uniform = false,
      ids = Seq(1),
      ratioOfCycleSize = 1.0,
      columns = Seq(
        "x0" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        },
        "x1" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        },
        "x2" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        },
        "x3" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        }
      ),
      numSlices = defaultPartitionParallelism,
      seed = 31415926L
    ).generate(),
    new TimeSeriesGenerator(
      sc,
      begin = 0L,
      end = cycles * frequency,
      frequency = frequency
    )(
      uniform = true,
      ids = Seq(1),
      ratioOfCycleSize = 1.0,
      columns = Seq(
        "x0" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble() - 1.0
        },
        "x1" -> { (_: Long, _: Int, rand: util.Random) =>
          -1.0 * rand.nextDouble()
        },
        "x2" -> { (_: Long, _: Int, rand: util.Random) =>
          10.0 * rand.nextDouble()
        },
        "x3" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble() + 1.0
        }
      ),
      numSlices = defaultPartitionParallelism,
      seed = 19811112L
    ).generate()
  )

  // Check if (a + b) + c = a + (b + c)
  class AssociativeLawProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      val p = timeSeriesRdd.rdd.partitions.length
      val maxDepth = Math.ceil(Math.log(p.toDouble) / Math.log(2.0)).toInt
      val summarizedResults = (1 to maxDepth).map { depth =>
        timeSeriesRdd
          .asInstanceOf[TimeSeriesRDDImpl]
          .summarizeInternal(summarizerFactory, Seq.empty, depth)
      }
      summarizedResults.foreach { result =>
        assertAlmostEquals(summarizedResults.head, result)
      }
    }

    override def toString: String = "AssociativeLawProperty"
  }

  // Check if 0 + 0 = 0
  class IdentityProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      val summarizer = summarizerFactory.apply(timeSeriesRdd.schema)
      val mergedZero = summarizer
        .merge(summarizer.zero(), summarizer.zero())
        .asInstanceOf[summarizer.U]
      val toExternalRow = CatalystTypeConvertersWrapper.toScalaRowConverter(
        summarizer.outputSchema
      )
      assertAlmostEquals(
        toExternalRow(summarizer.render(summarizer.zero())),
        toExternalRow(summarizer.render(mergedZero))
      )
    }

    override def toString: String = "IdentityProperty"
  }

  // Check if 0 + a = a
  class RightIdentityProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      val summarizer = summarizerFactory.apply(timeSeriesRdd.schema)
      val rows = timeSeriesRdd.toDF.queryExecution.toRdd.take(100)
      var nonZero = summarizer.zero()
      rows.foreach { row =>
        nonZero = summarizer.add(nonZero, row)
      }
      val toExternalRow = CatalystTypeConvertersWrapper.toScalaRowConverter(
        summarizer.outputSchema
      )
      val rightMerged =
        summarizer.merge(summarizer.zero(), nonZero).asInstanceOf[summarizer.U]
      assertAlmostEquals(
        toExternalRow(summarizer.render(nonZero)),
        toExternalRow(summarizer.render(rightMerged))
      )
    }

    override def toString: String = "RightIdentityProperty"
  }

  // Check if a + 0 = a
  class LeftIdentityProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      val summarizer = summarizerFactory.apply(timeSeriesRdd.schema)
      val rows = timeSeriesRdd.toDF.queryExecution.toRdd.map(_.copy).take(100)
      var nonZero = summarizer.zero()
      rows.foreach { row =>
        nonZero = summarizer.add(nonZero, row)
      }
      val toExternalRow = CatalystTypeConvertersWrapper.toScalaRowConverter(
        summarizer.outputSchema
      )
      val leftMerged =
        summarizer.merge(nonZero, summarizer.zero()).asInstanceOf[summarizer.U]
      assertAlmostEquals(
        toExternalRow(summarizer.render(nonZero)),
        toExternalRow(summarizer.render(leftMerged))
      )
    }

    override def toString: String = "LeftIdentityProperty"
  }

  // Check if (a + b) + c - a = b + c
  class LeftSubtractableProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      require(
        summarizerFactory
        .apply(timeSeriesRdd.schema)
        .isInstanceOf[LeftSubtractableSummarizer]
      )
      val summarizer = summarizerFactory
        .apply(timeSeriesRdd.schema)
        .asInstanceOf[LeftSubtractableSummarizer]

      val toExternalRow = CatalystTypeConvertersWrapper.toScalaRowConverter(
        summarizer.outputSchema
      )

      val rows = timeSeriesRdd.toDF.queryExecution.toRdd.map(_.copy).take(1000)
      var window = 11
      require(rows.length > window)

      while (window < rows.length) {
        var i = 0
        var s1 = summarizer.zero()

        // Build up state for the first window
        while (i < window) {
          s1 = summarizer.add(s1, rows(i))
          i += 1
        }

        while (i < rows.length) {
          s1 = summarizer.add(s1, rows(i))
          s1 = summarizer.subtract(s1, rows(i - window))

          // Build up benchmark state
          var s2 = summarizer.zero()
          var j = i - window + 1
          while (j <= i) {
            s2 = summarizer.add(s2, rows(j))
            j += 1
          }

          assertAlmostEquals(
            toExternalRow(summarizer.render(s1)),
            toExternalRow(summarizer.render(s2))
          )
          i += 1
        }
        window *= window
      }

    }

    override def toString: String = "LeftSubtractableProperty"
  }

  class WindowProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      require(
        summarizerFactory
        .apply(timeSeriesRdd.schema)
        .isInstanceOf[LeftSubtractableSummarizer]
      )

      val windowSize = 100L * frequency
      val window = Windows.pastAbsoluteTime(s"$windowSize ns")

      val end = timeSeriesRdd.toDF
        .select(TimeSeriesRDD.timeColumnName)
        .where(col(TimeSeriesRDD.timeColumnName) > (cycles - 2) * frequency)
        .collect()
        .last
        .getAs[Long](TimeSeriesRDD.timeColumnName)
      val lastWindow = timeSeriesRdd.deleteRows { r =>
        r.getAs[Long](TimeSeriesRDD.timeColumnName) < window.of(end)._1
      }

      val lastWindowSummarized = lastWindow.summarize(summarizerFactory)
      // We need to escape the column name to ensure that columns with names like "a.b" are handled well.
      val nonTimeColumnNames = lastWindowSummarized.schema.fieldNames.filterNot(_ == TimeSeriesRDD.timeColumnName).map {
        columnName => col(s"`$columnName`")
      }

      val expectedResults = lastWindowSummarized
        .toDF
        .select(nonTimeColumnNames: _*)
        .head

      val results = timeSeriesRdd
        .summarizeWindows(
          window,
          summarizerFactory
        )
        .toDF
        .where(col(TimeSeriesRDD.timeColumnName) === end)
        .select(nonTimeColumnNames: _*)
        .collect()
        .last

      assertAlmostEquals(results, expectedResults)
    }

    override def toString: String = "WindowProperty"
  }

  // Check if a - a = 0
  class SubtractIdentityProperty extends SummarizerProperty {
    override def test(
      timeSeriesRdd: TimeSeriesRDD,
      summarizerFactory: SummarizerFactory
    ): Unit = {
      require(
        summarizerFactory
        .apply(timeSeriesRdd.schema)
        .isInstanceOf[LeftSubtractableSummarizer]
      )
      val summarizer = summarizerFactory
        .apply(timeSeriesRdd.schema)
        .asInstanceOf[LeftSubtractableSummarizer]

      val rows = timeSeriesRdd.toDF.queryExecution.toRdd.map(_.copy).take(100)
      var subtracted = summarizer.zero()
      rows.foreach { row =>
        subtracted = summarizer.add(subtracted, row)
      }
      rows.foreach { row =>
        subtracted = summarizer.subtract(subtracted, row)
      }
      val toExternalRow = CatalystTypeConvertersWrapper.toScalaRowConverter(
        summarizer.outputSchema
      )

      assertAlmostEquals(
        toExternalRow(summarizer.render(subtracted)),
        toExternalRow(summarizer.render(summarizer.zero()))
      )
      // We could check that 0 - a throws an exception, but currently some summarizers do not check for this case.
    }

    override def toString: String = "SubtractIdentityProperty"
  }

  lazy val AllProperties = Seq(
    new AssociativeLawProperty,
    new RightIdentityProperty,
    new LeftIdentityProperty,
    new IdentityProperty
  )

  lazy val AllPropertiesAndSubtractable: Seq[SummarizerProperty] = AllProperties ++ Seq(
    new LeftSubtractableProperty,
    new WindowProperty,
    new SubtractIdentityProperty
  )

  def summarizerPropertyTest(properties: Seq[SummarizerProperty])(
    summarizer: SummarizerFactory
  ): Unit = {
    // Ensure data persists for all tests.
    AllData.foreach { data =>
      data.cache()
      data.count()
    }
    properties.foreach { property =>
      AllData.zipWithIndex.foreach {
        case (data, i) =>
          info(s"Satisfy property ${property.toString} with $i-th dataset")
          property.test(data, summarizer)
      }
    }
    AllData.foreach(_.unpersist())
  }

}
