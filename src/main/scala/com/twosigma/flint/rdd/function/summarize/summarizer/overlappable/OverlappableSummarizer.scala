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

package com.twosigma.flint.rdd.function.summarize.summarizer.overlappable

import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer

/**
 * Very similar to [[Summarizer]] interface. The only difference is that it has an additional `add` operator where
 * each row is associated with a flag to indicate whether it is overlapped.
 *
 * @note When an [[OverlappableSummarizer]] is applied, it should be treated exclusively either a [[Summarizer]] or
 *       a [[OverlappableSummarizer]], i.e. only one of the `add` operators could be invoked during the summarization.
 *       Thus the implementation of [[OverlappableSummarizer]] should decide if it is safe to use an
 *       [[OverlappableSummarizer]] as a [[Summarizer]] which could give meaningful result.
 */
trait OverlappableSummarizer[T, U, V] extends Summarizer[T, U, V] {

  def add(u: U, t: (T, Boolean)): U
}
