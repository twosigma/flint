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

package com.twosigma.flint

private[flint] object FlintConf {
  // Max batch size in summarizeWindowsBatch
  val WINDOW_BATCH_MAXSIZE_CONF = "spark.flint.window.batch.maxSize"
  // TODO: Fine tune this.
  val WINDOW_BATCH_MAXSIZE_DEFAULT = "500000"

  // Whether to use nanos or timestamp for the time column
  // Supported value: long or timestamp
  val TIME_TYPE_CONF = "spark.flint.timetype"
  val TIME_TYPE_DEFAULT = "long"
}
