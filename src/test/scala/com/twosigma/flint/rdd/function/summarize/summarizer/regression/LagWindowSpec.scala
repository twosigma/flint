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

package com.twosigma.flint.rdd.function.summarize.summarizer.regression

import org.scalatest.FlatSpec

class LagWindowSpec extends FlatSpec {

  "LagWindow" should "give an `AbsoluteTimeLagWindow` as expect" in {
    val lagWindow = LagWindow.absolute(10L)
    assert(!lagWindow.shouldKeep(20L, 10L, 0))
    assert(!lagWindow.shouldKeep(20L, 10L, Int.MaxValue))
    assert(lagWindow.shouldKeep(15L, 10L, 0))
    assert(lagWindow.shouldKeep(15L, 10L, Int.MaxValue))
    assert(!lagWindow.shouldKeep(25L, 10L, 0))
    assert(!lagWindow.shouldKeep(25L, 10L, Int.MaxValue))
  }

  it should "give a `CountLagWindow` as expect" in {
    val countLagWindow = LagWindow.count(3, 10)
    assert(countLagWindow.shouldKeep(20L, 10L, 0))
    assert(countLagWindow.shouldKeep(20L, 10L, 1))
    assert(countLagWindow.shouldKeep(20L, 10L, 2))
    assert(countLagWindow.shouldKeep(20L, 10L, 3))
    assert(countLagWindow.shouldKeep(20L, 10L, 4))
    assert(!countLagWindow.shouldKeep(20L, 10L, 5))

    assert(countLagWindow.shouldKeep(15L, 10L, 0))
    assert(countLagWindow.shouldKeep(15L, 10L, 1))
    assert(countLagWindow.shouldKeep(15L, 10L, 2))
    assert(countLagWindow.shouldKeep(15L, 10L, 3))
    assert(countLagWindow.shouldKeep(15L, 10L, 4))
    assert(!countLagWindow.shouldKeep(15L, 10L, 5))

    assert(!countLagWindow.shouldKeep(25L, 10L, 0))
    assert(!countLagWindow.shouldKeep(25L, 10L, 1))
    assert(!countLagWindow.shouldKeep(25L, 10L, 2))
    assert(!countLagWindow.shouldKeep(25L, 10L, 3))
    assert(!countLagWindow.shouldKeep(25L, 10L, 4))
    assert(!countLagWindow.shouldKeep(25L, 10L, 5))
  }

  "LagWindowQueue" should "work with an `AbsoluteTimeLagWindow` as expect" in {
    val window = LagWindow.absolute(10L)
    val lagWindowQueue = new LagWindowQueue[Int](window)
    lagWindowQueue.enqueue(0, 0)
    lagWindowQueue.enqueue(5, 5)
    lagWindowQueue.enqueue(10, 10)
    assert(lagWindowQueue.length == 2)
    assert(lagWindowQueue.head.timestamp == 5L)
    assert(lagWindowQueue.last.timestamp == 10L)
    lagWindowQueue.enqueue(100, 100)
    assert(lagWindowQueue.length == 1)
    assert(lagWindowQueue.last.timestamp == 100L)
  }

  it should "work with a `CountLagWindow` as expect" in {
    val window = LagWindow.count(3, 10L)
    val lagWindowQueue = new LagWindowQueue[Int](window)
    lagWindowQueue.enqueue(0, 0)
    lagWindowQueue.enqueue(5, 5)
    lagWindowQueue.enqueue(7, 7)
    lagWindowQueue.enqueue(10, 10)
    lagWindowQueue.enqueue(10, 10)

    assert(lagWindowQueue.length == 4)
    assert(lagWindowQueue.head.timestamp == 0L)
    assert(lagWindowQueue.last.timestamp == 10L)

    lagWindowQueue.enqueue(11, 11)
    assert(lagWindowQueue.length == 4)
    assert(lagWindowQueue.head.timestamp == 5L)

    lagWindowQueue.enqueue(100, 100)
    assert(lagWindowQueue.length == 1)
    assert(lagWindowQueue.head.timestamp == 100)

    lagWindowQueue.clear()
    assert(lagWindowQueue.length == 0)
  }
}
