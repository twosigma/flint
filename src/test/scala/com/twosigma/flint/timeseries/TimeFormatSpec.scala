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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.time.TimeFormat
import org.joda.time.DateTimeZone
import org.scalatest.FlatSpec

class TimeFormatSpec extends FlatSpec {

  "TimeFormat" should "parse time correctly" in {
    assert(TimeFormat.parseNano("1970-01-01") == 0L)
    assert(TimeFormat.parseNano("1970-01-01 00:00:00.000 +0000", DateTimeZone.forOffsetHours(2)) == 0L)
    assert(TimeFormat.parseNano("1970-01-01 00:00:00.000 -1000", DateTimeZone.forOffsetHours(1)) == 36000000000000L)
    assert(TimeFormat.parseNano("1970-01-01 00:00:00.000 -1000", DateTimeZone.forOffsetHours(3)) == 36000000000000L)
  }
}
