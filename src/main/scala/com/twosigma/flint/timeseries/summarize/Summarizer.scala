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

import scala.reflect.runtime.universe.{ TypeTag, typeTag }
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.{ OverlappableSummarizer => OOverlappableSummarizer }
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ LeftSubtractableSummarizer => OLeftSubtractableSummarizer, LeftSubtractableOverlappableSummarizer => OLeftSubtractableOverlappableSummarizer }
import com.twosigma.flint.rdd.function.summarize.summarizer.{ FlippableSummarizer => OFlippableSummarizer, Summarizer => OSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.summarizer.PredicateSummarizerFactory
import com.twosigma.flint.timeseries.window.TimeWindow
import org.apache.spark.sql.CatalystTypeConvertersWrapper
import org.apache.spark.sql.catalyst.{ InternalRow, ScalaReflection }
import org.apache.spark.sql.types.StructType

import scala.util.Try

trait InputOutputSchema {
  /**
   * The schema of input rows.
   */
  val inputSchema: StructType

  /**
   * The schema of output rows. The output schema will be exactly this `schema` if `alias` is `None`.
   * Otherwise, it will be prepend the alias.
   */
  val schema: StructType

  /**
   * The prefixes of column names in the output schema.
   */
  val prefixOpt: Option[String]

  /**
   * Required input columns of the summarizer. This is used in column pruning and input filtering.
   */
  val requiredColumns: ColumnList

  /**
   * The schema of output rows.
   */
  final def outputSchema: StructType = prefixOpt.fold(schema) {
    prefix =>
      Schema.of(schema.map {
        field => s"${prefix}_${field.name}" -> field.dataType
      }: _*)
  }
}

// The purpose of using factory pattern here is to assemble the schema(s) of rows in runtime. When a user wants to
// user a particular summarizer over a [[TimeSeriesRDD]], he/she could not provide the schema until passing to the
// [[TimeSeriesRDD]] which holds the schema.
trait SummarizerFactory {

  protected var prefixOpt: Option[String] = None

  /**
   * Add prefix to the column names of output schema. All columns names will be prepended as format
   * "<prefix>_<column>".
   *
   * @param prefix The string that serves as prefix for the columns names of output schema.
   * @return a [[SummarizerFactory]] with the given prefix.
   */
  def prefix(prefix: String): SummarizerFactory = {
    prefixOpt = Option(prefix)
    this
  }

  /**
   * Return a summarizer with the given input schema.
   *
   * @param inputSchema The input schema to the summarizer
   * @return a summarizer with the given input schema.
   */
  def apply(inputSchema: StructType): Summarizer

  /**
   * Return a [[ColumnList]] that can be used to optimize computations.
   *
   * @return [[ColumnList.Sequence]] of column names used by the summarizer.
   */
  val requiredColumns: ColumnList

  /**
   * Return a new [[SummarizerFactory]] that skips all rows which don't satisfy the predicate function.
   *
   * @param f The filtering predicate.
   * @param columns A list of columns that will be used as input values for the predicate.
   * @return a new [[SummarizerFactory]] that will be applied only to filtered rows.
   */
  def where[A1: TypeTag](f: (A1) => Boolean)(columns: String*): SummarizerFactory = {
    require(columns.size == 1)
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).getOrElse(Nil)
    new PredicateSummarizerFactory(this, f, columns.zip(inputTypes))
  }

  def where[A1: TypeTag, A2: TypeTag](
    f: (A1, A2) => Boolean
  )(columns: String*): SummarizerFactory = {
    require(columns.size == 2)
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType
      :: ScalaReflection.schemaFor(typeTag[A2]).dataType :: Nil).getOrElse(Nil)
    new PredicateSummarizerFactory(this, f, columns.zip(inputTypes))
  }

  def where[A1: TypeTag, A2: TypeTag, A3: TypeTag](
    f: (A1, A2, A3) => Boolean
  )(columns: String*): SummarizerFactory = {
    require(columns.size == 3)
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType
      :: ScalaReflection.schemaFor(typeTag[A2]).dataType
      :: ScalaReflection.schemaFor(typeTag[A3]).dataType :: Nil).getOrElse(Nil)
    new PredicateSummarizerFactory(this, f, columns.zip(inputTypes))
  }

  def where[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](
    f: (A1, A2, A3, A4) => Boolean
  )(columns: String*): SummarizerFactory = {
    require(columns.size == 4)
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType
      :: ScalaReflection.schemaFor(typeTag[A2]).dataType
      :: ScalaReflection.schemaFor(typeTag[A3]).dataType
      :: ScalaReflection.schemaFor(typeTag[A4]).dataType :: Nil).getOrElse(Nil)
    new PredicateSummarizerFactory(this, f, columns.zip(inputTypes))
  }
}

/**
 * A [[SummarizerFactory]] base class that takes a list of input cols and set requiredColumns to them.
 */
abstract class BaseSummarizerFactory(cols: String*) extends SummarizerFactory {
  override val requiredColumns: ColumnList = ColumnList.Sequence(cols)
}

/**
 * A trait that defines input row filtering.
 * Child trait/class can implement it's own input row fitlering, for instance [[FilterNullInput]] and
 * [[InputAlwaysValid]]
 */
trait InputValidation {
  def isValid(r: InternalRow): Boolean
}

/**
 * A trait that implements input row filtering.
 * An input row is filtered if any of the required columns is null.
 * If requiredColumns is [[ColumnList.All]], input rows will NOT be filtered.
 */
trait FilterNullInput extends InputOutputSchema with InputValidation {
  // Indices of required input columns. If any of the input column is null, the row will be skipped.
  final lazy val requiredInputIndices: Array[Int] = requiredColumns match {
    // If ColumnList.All, it means we are adding the entire row, so we don't want to do any null filtering.
    case ColumnList.All => Array.empty
    case ColumnList.Sequence(columns) => columns.map(inputSchema.fieldIndex).toArray
  }

  @inline
  def isValid(r: InternalRow): Boolean = {
    var i = 0
    var hasNull = false
    while (!hasNull && i < requiredInputIndices.length) {
      if (r.isNullAt(requiredInputIndices(i))) {
        hasNull = true
      }
      i += 1
    }
    !hasNull
  }
}

trait InputAlwaysValid extends InputValidation {
  @inline
  def isValid(r: InternalRow): Boolean = true
}

trait Summarizer extends OSummarizer[InternalRow, Any, InternalRow] with InputValidation with InputOutputSchema {
  // The type of each row expected to
  type T

  // The type of summarizer internal state
  type U

  // The type of summarizer output
  type V

  val summarizer: OSummarizer[T, U, V]

  // Convert the InternalRow to the type of row expected by the `summarizer`.
  def toT(r: InternalRow): T

  // Convert the output of `summarizer` to the InternalRow.
  def fromV(v: V): InternalRow

  final protected def toU(any: Any): U = any.asInstanceOf[U]

  final override def zero(): Any = summarizer.zero()

  final override def add(u: Any, r: InternalRow): Any = if (isValid(r)) summarizer.add(toU(u), toT(r)) else u

  final override def merge(u1: Any, u2: Any): Any = summarizer.merge(toU(u1), toU(u2))

  final override def render(u: Any): InternalRow = fromV(summarizer.render(toU(u)))

  final override def close(u: Any): Unit = summarizer.close(toU(u))
}

trait FlippableSummarizer extends Summarizer with OFlippableSummarizer[InternalRow, Any, InternalRow] {

  override val summarizer: OFlippableSummarizer[T, U, V]
}

trait LeftSubtractableSummarizer extends Summarizer with OLeftSubtractableSummarizer[InternalRow, Any, InternalRow] {

  override val summarizer: OLeftSubtractableSummarizer[T, U, V]

  final override def subtract(u: Any, r: InternalRow): Any =
    if (isValid(r)) summarizer.subtract(toU(u), toT(r)) else u
}

trait OverlappableSummarizerFactory extends SummarizerFactory {
  override def apply(inputSchema: StructType): OverlappableSummarizer

  val window: TimeWindow
}

trait OverlappableSummarizer extends Summarizer
  with OOverlappableSummarizer[InternalRow, Any, InternalRow]
  with InputOutputSchema {
  type T
  type U
  type V
  val summarizer: OOverlappableSummarizer[T, U, V]

  final override def addOverlapped(u: Any, r: (InternalRow, Boolean)): Any =
    if (isValid(r._1)) summarizer.addOverlapped(toU(u), (toT(r._1), r._2)) else u
}

trait LeftSubtractableOverlappableSummarizer extends OverlappableSummarizer
  with OLeftSubtractableOverlappableSummarizer[InternalRow, Any, InternalRow]
  with InputOutputSchema {
  type T
  type U
  type V
  val summarizer: OLeftSubtractableOverlappableSummarizer[T, U, V]

  final override def subtractOverlapped(u: Any, r: (InternalRow, Boolean)): Any =
    if (isValid(r._1)) summarizer.subtractOverlapped(toU(u), (toT(r._1), r._2)) else u
}

trait LeftSubtractableOverlappableSummarizerFactory extends OverlappableSummarizerFactory {
  override def apply(inputSchema: StructType): LeftSubtractableOverlappableSummarizer

  val window: TimeWindow
}
