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

package com.twosigma.flint.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ Counter, TaskAttemptID, Job, TaskAttemptContext }

// This exists just because of a quirk of the record reader api.
case class ConfOnlyTAC(_conf: Configuration) extends Job with TaskAttemptContext {
  // JobContextImpl and JobContext
  override def getConfiguration: Configuration = _conf

  // TaskAttemptContext
  override def getTaskAttemptID: TaskAttemptID = sys.error("not implemented")
  override def setStatus(msg: String): Unit = sys.error("not implemented")
  override def getStatus = sys.error("not implemented")
  override def getProgress: Float = sys.error("not implemented")
  override def getCounter(counterName: Enum[_]): Counter = sys.error("not implemented")
  override def getCounter(groupName: String, counterName: String): Counter = sys.error("not implemented")

  // Progressable
  override def progress(): Unit = sys.error("not implemented")
}
