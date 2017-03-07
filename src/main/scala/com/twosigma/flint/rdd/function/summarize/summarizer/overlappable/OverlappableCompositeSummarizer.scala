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

import com.twosigma.flint.rdd.function.summarize.summarizer.CompositeSummarizer

class OverlappableCompositeSummarizer[T1, U1, V1, T2, U2, V2](
  override val summarizer1: OverlappableSummarizer[T1, U1, V1],
  override val summarizer2: OverlappableSummarizer[T2, U2, V2]
) extends CompositeSummarizer[T1, U1, V1, T2, U2, V2](summarizer1, summarizer2)
  with OverlappableSummarizer[(T1, T2), (U1, U2), (V1, V2)] {

  def addOverlapped(u: (U1, U2), t: ((T1, T2), Boolean)): (U1, U2) =
    (summarizer1.addOverlapped(u._1, (t._1._1, t._2)), summarizer2.addOverlapped(u._2, (t._1._2, t._2)))
}
