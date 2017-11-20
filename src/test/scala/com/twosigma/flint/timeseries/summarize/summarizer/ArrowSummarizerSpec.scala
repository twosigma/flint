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

package com.twosigma.flint.timeseries.summarize.summarizer

import java.io.File

import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesRDD }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.ArrowTestUtils
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowFileReader
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.{ ByteArrayReadableSeekableByteChannel, Validator }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{ FlintTestData, Row }
import org.apache.spark.sql.types._

class ArrowSummarizerSpec extends SummarizerSuite with FlintTestData {
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/arrowsummarizer"

  var priceTSRdd: TimeSeriesRDD = _

  import ArrowSummarizer._

  private lazy val init = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
  }

  "ArrowSummarizer" should "summarize cycles correctly" in {
    init
    val result = priceTSRdd.summarizeCycles(Summarizers.arrow(Seq("time", "price"), includeBaseRows = false))

    val jsonFile = withResource(s"$defaultResourceDir/Price.json") { filename =>
      new File(filename)
    }

    val allocator = new RootAllocator(Long.MaxValue)

    val jsonReader = new JsonFileReader(jsonFile, allocator)
    val jsonSchema = jsonReader.start()
    val jsonRoot = jsonReader.read()

    val row = result.first()
    val bytes = row.getAs[Array[Byte]](arrowBatchColumnName)
    val inputChannel = new ByteArrayReadableSeekableByteChannel(bytes)
    val reader = new ArrowFileReader(inputChannel, allocator)
    val root = reader.getVectorSchemaRoot

    val schema = root.getSchema
    reader.loadNextBatch()

    Validator.compareSchemas(schema, jsonSchema)
    Validator.compareVectorSchemaRoot(root, jsonRoot)

    jsonRoot.close()
    jsonReader.close()
    root.close()
    allocator.close()
  }

  it should "prune columns correctly" in {
    init
    val result1 = priceTSRdd.summarizeCycles(Summarizers.arrow(Seq("price"), includeBaseRows = false))
    val row1 = ArrowTestUtils.fileFormatToRows(result1.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema1 = StructType(Seq(StructField("price", DoubleType)))
    assertEquals(row1, new GenericRowWithSchema(Array(0.5), schema1))

    val result2 = priceTSRdd.summarizeCycles(Summarizers.arrow(Seq("time", "id", "price"), includeBaseRows = false))
    val row2 = ArrowTestUtils.fileFormatToRows(result2.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema2 = StructType(Seq(
      StructField("time", LongType),
      StructField("id", IntegerType),
      StructField("price", DoubleType)
    ))
    assertEquals(row2, new GenericRowWithSchema(Array(1000, 7, 0.5), schema2))

    val result3 = priceTSRdd.summarizeCycles(Summarizers.arrow(Seq("time", "price"), includeBaseRows = false))
    val row3 = ArrowTestUtils.fileFormatToRows(result3.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema3 = StructType(Seq(
      StructField("time", LongType),
      StructField("price", DoubleType)
    ))
    assertEquals(row3, new GenericRowWithSchema(Array(1000, 0.5), schema3))

    val result4 = priceTSRdd.summarizeCycles(
      Summarizers.arrow(
        Seq("time", "price"),
        includeBaseRows = false
      ),
      key = Seq("id")
    )
    val row4 = ArrowTestUtils.fileFormatToRows(result4.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema4 = StructType(Seq(
      StructField("time", LongType),
      StructField("price", DoubleType)
    ))
    assertEquals(row4, new GenericRowWithSchema(Array(1000, 0.5), schema4))

    val result5 = priceTSRdd.summarizeCycles(
      Summarizers.arrow(
        Seq("price"),
        includeBaseRows = false
      ),
      key = Seq("id")
    )
    val row5 = ArrowTestUtils.fileFormatToRows(result5.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema5 = StructType(Seq(
      StructField("price", DoubleType)
    ))
    assertEquals(row5, new GenericRowWithSchema(Array(0.5), schema5))

    val result6 = priceTSRdd.summarizeCycles(
      Summarizers.arrow(
        Seq("price"),
        includeBaseRows = true
      )
    )
    val row6 = ArrowTestUtils.fileFormatToRows(result6.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema6 = StructType(Seq(
      StructField("price", DoubleType)
    ))
    assertEquals(row6, new GenericRowWithSchema(Array(0.5), schema6))

    val result7 = priceTSRdd.summarizeCycles(
      Summarizers.arrow(
        Seq("time", "price"),
        includeBaseRows = true
      )
    )
    val row7 = ArrowTestUtils.fileFormatToRows(result7.first().getAs[Array[Byte]](arrowBatchColumnName)).head
    val schema7 = StructType(Seq(
      StructField("time", LongType),
      StructField("price", DoubleType)
    ))
    assertEquals(row7, new GenericRowWithSchema(Array(1000, 0.5), schema7))

  }

  it should "include baseRows correctly" in {
    init
    val tsrdd = priceTSRdd.addColumns("v" -> DoubleType -> { _ => 1.0 })
    val result = tsrdd.summarizeCycles(Summarizers.arrow(Seq("price"), includeBaseRows = true))
    assert(result.collect()(0).getAs[Seq[Row]](baseRowsColumnName).sameElements(tsrdd.collect()))
  }

  it should "handle exception correctly" in {
    init
    val fakeErrorMessage = "Fake error message"

    val table = priceTSRdd.addColumns("price1" -> DoubleType -> { row =>
      if (row.getAs[Double]("price") >= 3.0) {
        throw new RuntimeException(fakeErrorMessage)
      } else {
        row.getAs[Double]("price")
      }
    })

    val result = table.summarizeCycles(Summarizers.arrow(Seq("price1"), includeBaseRows = false))

    val thrown = intercept[org.apache.spark.SparkException] {
      result.toDF.show()
    }

    assert(thrown.getCause.getMessage == fakeErrorMessage)
  }
}
