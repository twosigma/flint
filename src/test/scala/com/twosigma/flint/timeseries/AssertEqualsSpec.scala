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
import org.apache.spark.sql.catalyst.expressions.{ GenericRowWithSchema => SqlRow }
import org.apache.spark.sql.types.{ ArrayType, DoubleType }
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable

class AssertEqualsSpec extends TimeSeriesSuite {
  "TimeSeriesSuite" should "assertEquals for two sql rows of DoubleType correctly" in {
    val schema = Schema("x" -> DoubleType)
    val row1 = new SqlRow(Array(1L, 1.0), schema)
    val row2 = new SqlRow(Array(1L, 1.0 + defaultAdditivePrecision * 0.1), schema)
    val row3 = new SqlRow(Array(1L, 1.0 + defaultAdditivePrecision * 10.0), schema)
    assertAlmostEquals(row1, row2)
    intercept[TestFailedException] {
      assertAlmostEquals(row1, row3)
    }
  }

  it should "assertEquals for two sql rows of ArrayType(DoubleType) correctly" in {
    val schema = Schema("x" -> ArrayType(DoubleType))
    val row1: Row = new SqlRow(Array(1L, mutable.WrappedArray.make(Array(1.0))), schema)
    val row2: Row = new SqlRow(
      Array(1L, mutable.WrappedArray.make(Array(1.0 + defaultAdditivePrecision * 0.1))), schema
    )
    val row3: Row = new SqlRow(
      Array(1L, mutable.WrappedArray.make(Array(1.0 + defaultAdditivePrecision * 10.0))), schema
    )
    assertAlmostEquals(row1, row2)
    intercept[TestFailedException] {
      assertAlmostEquals(row1, row3)
    }
  }

  it should "assertEquals for two sql rows of ArrayType(DoubleType) that contain NaN values correctly" in {
    val schema = Schema("x" -> ArrayType(DoubleType))
    val row1 = new SqlRow(Array(1L, mutable.WrappedArray.make(Array(Double.NaN))), schema)
    val row2 = new SqlRow(Array(1L, mutable.WrappedArray.make(Array(Double.NaN))), schema)
    val row3 = new SqlRow(Array(1L, mutable.WrappedArray.make(Array(1.0))), schema)
    assertAlmostEquals(row1, row2)
    intercept[TestFailedException] {
      assertAlmostEquals(row1, row3)
    }
  }
}
