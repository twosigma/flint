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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.SharedSparkContext
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow

class TreeReduceSpec extends FlatSpec with SharedSparkContext {

  "TreeReduce" should "reduce in order for max op" taggedAs (Slow) in {
    val numOfPartitions = 1023
    val scale = 5
    val maxDepth = 5
    val rdd = sc.parallelize(1 to numOfPartitions, numOfPartitions).mapPartitionsWithIndex {
      (idx, iter) => (1 to scale).map { x => idx * scale + x }.toIterator
    }

    // Use -1 as a "bad" state and propagate through the aggregation, otherwise
    // it is just simply a Math.max() operator.
    val op = (u1: Int, u2: Int) => if (u1 >= u2 || u1 < 0 || u2 < 0) {
      -1
    } else {
      u2
    }

    val expectedReducedResult = rdd.max()

    (1 to maxDepth).foreach {
      depth => assert(TreeReduce(rdd)(op, depth) == expectedReducedResult)
    }
  }

  it should "reduce in order for string concat" taggedAs (Slow) in {
    val numOfPartitions = 1111
    val scale = 5
    val maxDepth = 5
    val rdd = sc.parallelize(1 to numOfPartitions, numOfPartitions).mapPartitionsWithIndex {
      (idx, iter) => (1 to scale).map { x => s"${idx * scale + x}" }.toIterator
    }

    val f = (u1: String, u2: String) => u1 + u2
    val expectedReducedResult = rdd.collect().mkString("")

    (1 to maxDepth).foreach {
      depth => assert(TreeReduce(rdd)(f, depth) == expectedReducedResult)
    }
  }

  it should "reduce in order for sum op" taggedAs (Slow) in {
    val numOfPartitions = 1023
    val scale = 5
    val maxDepth = 5
    val rdd = sc.parallelize(1 to numOfPartitions, numOfPartitions).mapPartitionsWithIndex {
      (idx, iter) => (1 to scale).map { x => idx * scale + x }.toIterator
    }

    val expectedReducedResult = rdd.sum()

    val f = (u1: Int, u2: Int) => u1 + u2

    (1 to maxDepth).foreach {
      depth => assert(TreeReduce(rdd)(f, depth) == expectedReducedResult)
    }
  }

}
