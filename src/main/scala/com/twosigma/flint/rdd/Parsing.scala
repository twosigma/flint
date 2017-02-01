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

package com.twosigma.flint.rdd

import java.text.{ DateFormat, SimpleDateFormat }

// TODO should this really live here like this?
object Parsing {
  // This is NOT thread safe, so we need to make sure to grab a new one where appropriate.
  def timeFormatStandard: DateFormat = new SimpleDateFormat("yyyyMMdd H:mm:ss.SSS")
  def timeFormatDashesPlusTimeZone: DateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:ss.SSS Z")
}
