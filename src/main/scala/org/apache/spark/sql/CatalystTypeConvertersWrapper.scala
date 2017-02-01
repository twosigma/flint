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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.{ CatalystTypeConverters, InternalRow }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.DataType

/**
 * Functions to convert Scala types to Catalyst types and back.
 */
object CatalystTypeConvertersWrapper {
  def toCatalystRowConverter(dataType: DataType): Row => InternalRow = {
    CatalystTypeConverters.createToCatalystConverter(dataType)(_).asInstanceOf[InternalRow]
  }

  def toScalaRowConverter(dataType: DataType): InternalRow => GenericRowWithSchema = {
    CatalystTypeConverters.createToScalaConverter(dataType)(_).asInstanceOf[GenericRowWithSchema]
  }

  def toCatalystConverter(dataType: DataType): Any => Any =
    CatalystTypeConverters.createToCatalystConverter(dataType)
}
