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

import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.{ OverlappableCompositeSummarizer => OOverlappableCompositeSummarizer }
import com.twosigma.flint.timeseries.summarize.{ ColumnList, InputAlwaysValid, OverlappableSummarizer, OverlappableSummarizerFactory }
import com.twosigma.flint.timeseries.window.TimeWindow
import org.apache.spark.sql.types.StructType

case class OverlappableCompositeSummarizerFactory(
  factory1: OverlappableSummarizerFactory,
  factory2: OverlappableSummarizerFactory
) extends OverlappableSummarizerFactory {

  require(factory1.window == factory2.window, s"Window ${factory1.window} isn't equal to ${factory2.window}.")
  override val window: TimeWindow = factory1.window
  override val requiredColumns = factory1.requiredColumns ++ factory2.requiredColumns

  def apply(inputSchema: StructType): OverlappableSummarizer = {
    val summarizer1 = factory1.apply(inputSchema)
    val summarizer2 = factory2.apply(inputSchema)

    new OverlappableCompositeSummarizer(inputSchema, prefixOpt, requiredColumns, summarizer1, summarizer2)
  }
}

class OverlappableCompositeSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  override val summarizer1: OverlappableSummarizer,
  override val summarizer2: OverlappableSummarizer
) extends CompositeSummarizer(inputSchema, prefixOpt, requiredColumns, summarizer1, summarizer2)
  with OverlappableSummarizer {

  override val summarizer = new OOverlappableCompositeSummarizer(summarizer1, summarizer2)
}
