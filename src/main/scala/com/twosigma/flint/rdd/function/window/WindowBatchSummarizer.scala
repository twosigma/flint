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

package com.twosigma.flint.rdd.function.window

/**
 * Interface used during window batch summarization.
 * See: [[com.twosigma.flint.rdd.function.summarize.WindowBatchIterator]]
 *
 * This summarizer maintains separate state for each secondary SK but keep them in a single state object,
 * sub state for each SK are usually maintained in a Map data structure indexed by SK.
 * Therefore, sk is required in all methods in this interface to specify which state the method is changing.
 * This summarizer uses mutable state for performance reason.
 *
 *
 */
trait WindowBatchSummarizer[K, SK, V, U, V2] extends Serializable {
  /**
   * Returns an empty state.
   */
  def zero(): U

  /**
   * Add a left row in the current batch. This function should be called only once for each left row.
   *
   * Note it's NOT valid to call [[addLeft()]] twice for the same SK without calling [[commitLeft()]]
   * in between.
   *
   * This is NOT valid:
   * addLeft(u, 1, left1)
   * addLeft(u, 1, left2)
   *
   * This is valid:
   * addLeft(u, 1, left1)
   * commitLeft(u, 1, left1)
   * addLeft(u, 1, left2)
   * commitLeft(u, 1, left2)
   *
   * It's also valid to call [[addLeft()]] twice for different SK without calling [[commitLeft()]]
   * in between.
   *
   * This is valid:
   * addLeft(u, 1, left1)
   * addLeft(u, 2, left2)
   * commitLeft(u, 1, left1)
   * commitLeft(u, 2, left2)
   *
   * Note [[commitLeft()]] doesn't check whether the row for the SK is the same as passed in [[addLeft()]],
   * it's the caller's responsibility to make sure that.
   */
  def addLeft(u: U, sk: SK, v: V): Unit

  /**
   * Finish a left row in the current batch, construct the window with right rows in the window of SK at the moment.
   *
   * This function should be called only once for each left row and must be called after [[addLeft()]].
   *
   * This should set begin/end indices for the left row.
   */
  def commitLeft(u: U, sk: SK, v: V): Unit

  /**
   * Add a right row to the window of SK.
   *
   * The right row will remain in the window of SK until [[subtractRight()]] is called.
   *
   * Note:
   *
   * It is valid to call this function before [[addLeft()]] since a right row can be added to a window that
   * has not started yet.
   *
   * addRight(u, 1, right1)
   * finishLeft(u, 2, left1)
   *
   * startLeft(u, 1, left2)
   * ...
   * finishLeft(u, 1, left2)
   * ...
   * subtractRight(u, 1, right1)
   * ...
   * finishLeft(U, 1, left3)
   *
   * In this case, right1 will be included to the window of left2 but NOT left3.
   *
   * This function also adds the right row in the state.
   *
   */
  def addRight(u: U, sk: SK, v: V): Unit

  /**
   * Subtract a right row from the window of SK.
   *
   * This function MUST be called in the same order of [[addRight()]] for each SK.
   *
   * See also: [[addRight()]]
   *
   */
  def subtractRight(u: U, sk: SK, v: V): Unit

  /**
   * Construct left batch, right batch and indices from the state.
   */
  def render(u: U): V2
}
