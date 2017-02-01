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

import org.scalatest.FlatSpec

class OrderedIteratorSpec extends FlatSpec {

  "OrderedKeyValueIterator" should "iterate through an ordered collection correctly" in {
    val data = (1 to 10).map { x => (x, x) }.toArray
    assert(data.deep == OrderedIterator(data.iterator).toArray.deep)
  }

  it should "filter on a range correctly" in {
    val data = (1 to 10).map { x => (x, x) }.toArray
    var range = CloseOpen(0, Some(1))
    assert(OrderedIterator(data.iterator).filterByRange(range).length == 0)

    range = CloseOpen(0, Some(2))
    assert(OrderedIterator(data.iterator).filterByRange(range).toArray.deep == Array((1, 1)).deep)

    range = CloseOpen(3, Some(5))
    assert(OrderedIterator(data.iterator).filterByRange(range).toArray.deep == Array((3, 3), (4, 4)).deep)

    range = CloseOpen(9, Some(20))
    assert(OrderedIterator(data.iterator).filterByRange(range).toArray.deep == Array((9, 9), (10, 10)).deep)

    range = CloseOpen(11, Some(20))
    assert(OrderedIterator(data.iterator).filterByRange(range).length == 0)
  }
}
