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

package com.twosigma.flint

import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.Suite

/**
 *  Manages a local `sc` variable, properly stopping it after each test.
 */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {

  self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkContext.stop(sc)
    sc = null
  }
}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
  }
}
