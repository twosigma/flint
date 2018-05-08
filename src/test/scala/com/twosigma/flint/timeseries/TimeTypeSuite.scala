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

package com.twosigma.flint.timeseries

import com.twosigma.flint.FlintConf
import org.apache.spark.sql.SparkSession

trait TimeTypeSuite {
  def withTimeType[T](conf: String*)(block: => Unit): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    conf.foreach {
      conf =>
        val savedConf = spark.conf.getOption(FlintConf.TIME_TYPE_CONF)
        spark.conf.set(FlintConf.TIME_TYPE_CONF, conf)
        try {
          block
        } finally {
          savedConf match {
            case None => spark.conf.unset(FlintConf.TIME_TYPE_CONF)
            case Some(oldConf) => spark.conf.set(FlintConf.TIME_TYPE_CONF, oldConf)
          }
        }
    }
  }

  def withAllTimeType(block: => Unit): Unit = {
    withTimeType("long", "timestamp")(block)
  }
}
