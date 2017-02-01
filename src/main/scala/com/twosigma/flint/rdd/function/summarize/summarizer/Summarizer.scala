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

package com.twosigma.flint.rdd.function.summarize.summarizer

/**
 * For a [[Summarizer]] s, we define the following notations:
 * 0 = s.zero()
 * u + a = s.add(u, a)
 * u1 + u2 = s.merge(u1, u2)
 */

trait Summarizer[T, U, V] extends Serializable {
  /**
   *  @return the initial state of this summarizer.
   */
  def zero(): U

  /**
   * Update a state with a given value. The original u can be changed after [[add()]].
   *
   * @param u The prior state.
   * @param t A value expected to use for updating the prior state.
   * @return updated state
   */
  def add(u: U, t: T): U

  /**
   * Merge two summarizer states. The original u1 and u2 can be changed after [[merge()]]
   *
   * For two sequences (a[1], a[2], ..., a[n]), (a'[1], a'[2], ..., a'[m]), and two states u1 and
   * u1 where
   * u1 = 0 + a[1] + ... + a[n]
   * u2 = 0 + a'[1] + ... + a'[m]
   *
   * It must satisfy the following condition.
   * u1 + u2 = 0 + a[1] + ... + a[n] + a'[1] + ... + a'[m]
   *
   * @param u1 A state expected to merge as the left hand side of merge operation.
   * @param u2 A state expected to merge as the right hand side of merge operation.
   * @return the merged state
   */
  def merge(u1: U, u2: U): U

  /**
   * Renders a state into the output type. The original u should NOT change after [[render()]]
   *
   * @param u state expected to render
   * @return a rendered value with desired type.
   */
  def render(u: U): V
}
