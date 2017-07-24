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
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowFileReader
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.{ ByteArrayReadableSeekableByteChannel, Validator }
import org.apache.spark.sql.FlintTestData
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class ArrowSummarizerSpec extends SummarizerSuite with FlintTestData {
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/arrowsummarizer"

  var priceTSRdd: TimeSeriesRDD = _

  private lazy val init = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
  }

  "ArrowSummarizer" should "summarize cycles correctly" in {
    init
    val result = priceTSRdd.summarizeCycles(Summarizers.arrow(Seq("price")))

    val jsonFile = withResource(s"$defaultResourceDir/Price.json") { filename =>
      new File(filename)
    }

    val allocator = new RootAllocator(Long.MaxValue)

    val jsonReader = new JsonFileReader(jsonFile, allocator)
    val jsonSchema = jsonReader.start()
    val jsonRoot = jsonReader.read()

    val row = result.first()
    val bytes = row.getAs[Array[Byte]]("arrow_bytes")
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

    val result = table.summarizeCycles(Summarizers.arrow(Seq("price1")))

    val thrown = intercept[org.apache.spark.SparkException] {
      result.toDF.show()
    }

    assert(thrown.getCause.getMessage == fakeErrorMessage)
  }
}
