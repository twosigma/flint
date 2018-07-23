/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

import java.util.Properties

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext, SQLImplicits, SparkSession }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/**
 * Shares a local `sc` and a local `sqlContext` between all tests in a suite and closes it at the end.
 */
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  @transient private var _sqlContext: SQLContext = _

  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _sc

  def sqlContext: SQLContext = _sqlContext

  def spark: SparkSession = _spark

  {
    // Set logging for our tests to WARN since tons of Spark statements that should be DEBUG are oddly listed as INFO.
    configTestLog4j("WARN")
  }

  var conf = new SparkConf(false)

  override def beforeAll() {
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.session.timeZone", "UTC")

    // Set the console progress if the system property is set
    sys.props.get("spark.ui.showConsoleProgress").foreach(
      conf.set("spark.ui.showConsoleProgress", _)
    )

    // we want to detect memory leaks as soon as possible
    conf.set("spark.unsafe.exceptionOnMemoryLeak", "true")
      // The codec used to compress internal data such as RDD partitions, broadcast variables and shuffle outputs.
      // By default, Spark provides three codecs: lz4, lzf, and snappy. Here, using lzf is to reduce the dependency
      // of snappy codec for compiling issue with other codebase(s).
      .set("spark.io.compression.codec", "lzf")
    val cores = sys.props.getOrElse(
      "spark.executor.cores",
      Math.ceil(sys.runtime.availableProcessors() * 0.75).toInt
    )

    _spark = SparkSession.builder().master(s"local[$cores]").appName("test").config(conf).getOrCreate()
    _sqlContext = _spark.sqlContext
    _sc = _spark.sparkContext

    super.beforeAll()
  }

  override def afterAll() {
    _spark.stop()
    _spark = null
    _sqlContext = null
    _sc = null

    super.afterAll()
  }

  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)
  }

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   *
   * @see https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/test/SQLTestUtils.scala#L165
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

}
