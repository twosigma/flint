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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

class GeometricMeanSummarizer extends ProductSummarizer {
  override def render(u: ProductState): Double = {
    if (u.count == 0L) {
      Double.NaN
    } else if (u.zeroCount > 0L) {
      0.0
    } else if (u.isPositive) {
      Math.exp(u.sumOfLogs.value / u.count)
    } else {
      -Math.exp(u.sumOfLogs.value / u.count)
    }
  }
}
