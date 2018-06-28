/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.concurrent.TimeUnit

import com.twosigma.flint.arrow.ArrowUtils
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.{ BigIntVector, Float8Vector, VectorSchemaRoot }
import org.apache.spark.sql.functions.{ array, col, lit, struct }
import org.apache.spark.sql.types._

class ConcatArrowAndExplodeSpec extends TimeSeriesSuite {

  "ConcatArrowAndExplode" should "work" in {

    val batchSize = 10

    var df = spark.range(1000, 2000, 1000).toDF("time")
    val columns = (0 until batchSize).map(v => struct((df("time") + v).as("time"), lit(v.toDouble).as("v")))
    df = df.withColumn("base_rows", array(columns: _*))

    val allocator = new RootAllocator(Long.MaxValue)

    val schema1 = StructType(Seq(StructField("v1", DoubleType)))
    val root1 = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema1), allocator)
    val vector1 = root1.getVector("v1").asInstanceOf[Float8Vector]
    vector1.allocateNew()

    for (i <- 0 until batchSize) {
      vector1.set(i, i + 10.0)
    }
    vector1.setValueCount(batchSize)
    val out1 = new ByteArrayOutputStream()
    val arrowWriter1 = new ArrowFileWriter(root1, null, Channels.newChannel(out1))
    arrowWriter1.writeBatch()
    arrowWriter1.close()
    root1.close()
    df = df.withColumn("f1_schema", struct(lit(0.0).as("v1")))
    df = df.withColumn("f1_data", lit(out1.toByteArray))

    val schema2 = StructType(Seq(StructField("v2", DoubleType), StructField("v3", LongType)))
    val root2 = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema2), allocator)
    val vector2 = root2.getVector("v2").asInstanceOf[Float8Vector]
    val vector3 = root2.getVector("v3").asInstanceOf[BigIntVector]
    vector2.allocateNew()
    vector3.allocateNew()

    for (i <- 0 until batchSize) {
      vector2.set(i, i + 20.0)
    }
    vector2.setValueCount(batchSize)

    for (i <- 0 until batchSize) {
      vector3.set(i, i + 30L)
    }
    vector3.setValueCount(batchSize)
    val out2 = new ByteArrayOutputStream()
    val arrowWriter2 = new ArrowFileWriter(root2, null, Channels.newChannel(out2))
    arrowWriter2.writeBatch()
    arrowWriter2.close()
    root2.close()
    df = df.withColumn("f2_schema", struct(lit(0.0).as("v2"), lit(0L).as("v3")))
    df = df.withColumn("f2_data", lit(out2.toByteArray))

    var tsrdd = TimeSeriesRDD.fromDF(df)(isSorted = false, timeUnit = TimeUnit.NANOSECONDS)
    tsrdd = tsrdd.concatArrowAndExplode("base_rows", Seq("f1_schema", "f2_schema"), Seq("f1_data", "f2_data"))
    tsrdd.toDF.show()

    var expected = spark.range(1000, 1000 + batchSize).toDF("time")
    expected = expected.withColumn("v", col("time") - 1000.0)
    expected = expected.withColumn("v1", col("time") - 1000 + 10.0)
    expected = expected.withColumn("v2", col("time") - 1000 + 20.0)
    expected = expected.withColumn("v3", col("time") - 1000 + 30)

    val expectedTsrdd = TimeSeriesRDD.fromDF(expected)(isSorted = false, timeUnit = TimeUnit.NANOSECONDS)
    assertEquals(tsrdd, expectedTsrdd)
  }

}
