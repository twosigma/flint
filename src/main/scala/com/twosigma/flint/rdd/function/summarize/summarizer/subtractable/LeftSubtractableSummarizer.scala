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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer

/**
 * With the following definitions and notations for a summarizer: s,<br>
 * u + a = s.add(u, a)<br>
 * u - a = s.subtract(u, a)<br>
 * <p>
 * For a summarizer s, if there exists a function s.subtract(u, a) such that for any sequence
 * (a[1], a[2] ..., a[n]), any prefix sequence (a[1], a[2], ..., a[m]) where m <= n, and any
 * state u, it satisfies<br>
 * u + a[1] + a[2] + ... + a[n] - a[1] - a[2] - ... - a[m] == u + a[m+1] + a[m+2] ... + a[n]<br>
 * then, we will say that the summarizer s is left-subtractable.
 */
trait LeftSubtractableSummarizer[V, U, V2] extends Summarizer[V, U, V2] {
  def subtract(u: U, v: V): U
}
