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

package com.twosigma.flint.sql.function

import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class RankSpec extends FlatSpec with BeforeAndAfterAll {
  val schema1 = StructType(
    StructField("time", LongType) ::
      StructField("id", IntegerType) ::
      StructField("price", DoubleType) :: Nil
  )

  "Rank" should "percent rank" in {
    val rows = Seq(
      new GenericRowWithSchema(Array(1000L, 5, 2.0), schema1),
      new GenericRowWithSchema(Array(1000L, 5, 2.0), schema1),
      new GenericRowWithSchema(Array(1000L, 5, 3.0), schema1),
      new GenericRowWithSchema(Array(1000L, 5, 1.0), schema1)
    )

    val schema = StructType(schema1.fields :+ StructField("r", DoubleType))
    val results = Seq(
      new GenericRowWithSchema(Array(1000L, 5, 2.0, 0.5), schema),
      new GenericRowWithSchema(Array(1000L, 5, 2.0, 0.5), schema),
      new GenericRowWithSchema(Array(1000L, 5, 3.0, 0.875), schema),
      new GenericRowWithSchema(Array(1000L, 5, 1.0, 0.125), schema)
    )

    assert(Rank.percentRank[Double](rows, "price", "r") == results)
  }
}
