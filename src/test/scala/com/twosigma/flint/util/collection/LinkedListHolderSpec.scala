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

package com.twosigma.flint.util.collection

import org.scalatest.FlatSpec
import java.util.{ LinkedList => JLinkedList }
import scala.collection.JavaConverters._

class LinkedListHolderSpec extends FlatSpec {

  "LinkedListHolder" should "dropWhile correctly" in {
    import Implicits._
    val l = new JLinkedList[Int]()
    val n = 10
    val p = 3
    l.addAll((1 to n).asJava)

    val (dropped1, rest1) = l.dropWhile { i => i > p }
    assert(dropped1.size() == 0)
    assert(rest1.toArray.deep == l.toArray.deep)

    val (dropped2, rest2) = l.dropWhile { i => i < p }
    assert(dropped2.toArray.deep == (1 until p).toArray.deep)
    assert(rest2.toArray.deep == (p to n).toArray.deep)
  }

  it should "foldLeft correctly" in {
    import com.twosigma.flint.util.collection.Implicits._
    val l = new JLinkedList[Int]()
    assert(l.foldLeft(1)(_ + _) == 1)
    val n = 10
    l.addAll((1 to n).asJava)
    assert(l.foldLeft(0)(_ + _) == 55)
  }
}
