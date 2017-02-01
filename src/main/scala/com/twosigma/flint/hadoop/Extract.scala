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

import org.apache.hadoop.io.Text

object Extract extends java.io.Serializable {
  def unit[T]: Extract[T] { type Extracted = Unit } =
    new Extract[T] {
      type Extracted = Unit
      override def apply(t: T) = ()
    }

  implicit val text: Extract[Text] { type Extracted = String } =
    new Extract[Text] {
      type Extracted = String
      override def apply(t: Text) = t.toString
    }
}

trait Extract[From] extends java.io.Serializable {
  type Extracted
  def apply(t: From): Extracted
}
