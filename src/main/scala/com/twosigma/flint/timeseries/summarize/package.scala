/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.reflect.{ ClassTag, classTag }

package object summarize {

  def asDoubleExtractor(dataType: DataType, columnIndex: Int): InternalRow => Double = dataType match {
    case DoubleType => { row: InternalRow => row.getDouble(columnIndex) }
    case LongType => { row: InternalRow => row.getLong(columnIndex).toDouble }
    case IntegerType => { row: InternalRow => row.getInt(columnIndex).toDouble }
    case FloatType => { row: InternalRow => row.getFloat(columnIndex).toDouble }
    case _ => throw new IllegalArgumentException(s"Cannot cast $dataType to DoubleType")
  }

  def toClassTag(dataType: DataType): ClassTag[_] = dataType match {
    case IntegerType => classTag[Int]
    case LongType => classTag[Long]
    case FloatType => classTag[Float]
    case DoubleType => classTag[Double]
    case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
  }

  def toOrdering(dataType: DataType): Ordering[_] = dataType match {
    case IntegerType => Ordering[Int]
    case LongType => Ordering[Long]
    case FloatType => Ordering[Float]
    case DoubleType => Ordering[Double]
    case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
  }
}
