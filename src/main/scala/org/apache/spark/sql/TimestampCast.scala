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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.{ Expression, NullIntolerant, UnaryExpression }
import org.apache.spark.sql.types.{ DataType, LongType, TimestampType }

case class TimestampToNanos(child: Expression) extends TimestampCast {
  val dataType: DataType = LongType
  protected def cast(childPrim: String): String =
    s"$childPrim * 1000L"
  override protected def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Long] * 1000L
}

case class NanosToTimestamp(child: Expression) extends TimestampCast {
  val dataType: DataType = TimestampType
  protected def cast(childPrim: String): String =
    s"$childPrim / 1000L"
  override protected def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Long] / 1000L
}

object TimestampToNanos {
  /** Public factory for constructing a Column */
  def apply(child: Column): Column = Column(TimestampToNanos(child.expr))
}

object NanosToTimestamp {
  /** Public factory for constructing a Column */
  def apply(child: Column): Column = Column(NanosToTimestamp(child.expr))
}

/**
 * Trait implementing an expression for casting Timestamp to / from
 * Long with microsecond precision.
 */
trait TimestampCast extends UnaryExpression with NullIntolerant {

  /**
   * A TimestampType or LongType column.
   */
  def child: Expression

  protected def cast(childPrim: String): String

  /** Copied and modified from org/apache/spark/sql/catalyst/expressions/Cast.scala */
  private[this] def castCode(ctx: CodegenContext, childPrim: String, childNull: String,
    resultPrim: String, resultNull: String, resultType: DataType): Block = {
    code"""
      boolean $resultNull = $childNull;
      ${JavaCode.javaType(resultType)} $resultPrim = ${CodeGenerator.defaultValue(resultType)};
      if (!${childNull}) {
        $resultPrim = (long) ${cast(childPrim)};
      }
    """
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ev.copy(code = eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType))
  }
}
