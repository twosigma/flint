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

package com.twosigma.flint.rdd.function.summarize.summarizer

case class CompositeSummarizer[T1, U1, V1, T2, U2, V2](
  summarizer1: Summarizer[T1, U1, V1],
  summarizer2: Summarizer[T2, U2, V2]
)
  extends Summarizer[(T1, T2), (U1, U2), (V1, V2)] {

  override def zero(): (U1, U2) = (summarizer1.zero(), summarizer2.zero())

  override def merge(u1: (U1, U2), u2: (U1, U2)): (U1, U2) = {
    (summarizer1.merge(u1._1, u2._1), summarizer2.merge(u1._2, u2._2))
  }

  override def render(u: (U1, U2)): (V1, V2) =
    (summarizer1.render(u._1), summarizer2.render(u._2))

  override def add(u: (U1, U2), t: (T1, T2)): (U1, U2) =
    (summarizer1.add(u._1, t._1), summarizer2.add(u._2, t._2))
}
