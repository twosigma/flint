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

import scala.collection.mutable

case class LagElement[E](timestamp: Long, element: E)

class LagWindowQueue[E](
  lagWindow: LagWindow
) extends Serializable {

  private val queue: mutable.Queue[LagElement[E]] = new mutable.Queue[LagElement[E]]()

  private def isNew(timestamp: Long): Boolean = queue.nonEmpty && queue.last.timestamp != timestamp

  def enqueue(timeStamp: Long, e: E): LagElement[E] = {
    if (queue.isEmpty || isNew(timeStamp)) {
      queue.enqueue(LagElement(timeStamp, e))
      while (queue.size > 1 && !lagWindow.shouldKeep(timeStamp, queue.head.timestamp, queue.size)) {
        queue.dequeue()
      }
    }
    queue.head
  }

  /**
   * @return E_{t - j}, E_{t - j + 1}, ..., E_{t - 1}, E_t
   */
  def toArray: Array[LagElement[E]] = queue.toArray

  def length: Int = queue.length

  def head: LagElement[E] = queue.head

  def last: LagElement[E] = queue.last

  def clear(): Unit = queue.clear()

}

object LagWindowQueue {
  def apply[E](window: LagWindow) = new LagWindowQueue[E](window)
}
