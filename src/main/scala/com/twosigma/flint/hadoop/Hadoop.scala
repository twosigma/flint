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

package com.twosigma.flint.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

import com.twosigma.flint.rdd.Range
import grizzled.slf4j.Logger

object Hadoop {

  val logger = Logger(Hadoop.getClass)

  def fileSplits[K1, V1, K: Ordering](
    sc: SparkContext,
    file: String,
    ifConf: InputFormatConf[K1, V1] // TODO consider just straight up making this (K, K) as we CAN get it, it's just a pain.
  )(parseKey: (ifConf.KExtract#Extracted, ifConf.VExtract#Extracted) => K): Map[Int, (Range[K], WriSer[ifConf.Split])] = {
    val splits = ifConf.makeSplits(new Configuration())
    logger.info(s"Total number of splits: ${splits.size}")
    splits.foreach { s => logger.debug(s.get.toString) }
    // TODO implement the version which does the more rigorous thing, at least for splits that
    // support it
    val m = getSplitTimes(sc, ifConf)(parseKey, splits)
      .sortBy(_._1)
      .zip(splits)
      .map { case ((index, time), split) => (index, (time, split)) }
      .toMap
    m.map { case (k, (b, w)) => (k, (Range(b, m.get(k + 1).map(_._1)), w)) }
  }

  def getSplitTimes[K1, V1, K](
    sc: SparkContext,
    ifConf: InputFormatConf[K1, V1]
  )(
    parseKey: (ifConf.KExtract#Extracted, ifConf.VExtract#Extracted) => K,
    splits: Seq[WriSer[ifConf.Split]]
  ): Vector[(Int, K)] =
    sc.parallelize(splits.zipWithIndex).map {
      case (serSplit, num) =>
        val (a, b) = readRecords(ifConf)(serSplit).next
        val time = parseKey(a, b)
        Vector((num, time))
    }.reduce(_ ++ _)

  def readRecords[K, V](ifConf: InputFormatConf[K, V])(
    serSplit: WriSer[ifConf.Split]
  ): Iterator[(ifConf.KExtract#Extracted, ifConf.VExtract#Extracted)] = {
    val inputFormat = ifConf.makeInputFormat()
    val split = serSplit.get

    val tac = ConfOnlyTAC(new Configuration())
    val recordReader = inputFormat.createRecordReader(split, tac)
    recordReader.initialize(split, tac)

    logger.info(s"Beginning to read lines from split: $split")
    new Iterator[(ifConf.KExtract#Extracted, ifConf.VExtract#Extracted)] {
      var stillMore = false
      lazy val init = stillMore = recordReader.nextKeyValue()

      override def hasNext = {
        init
        stillMore
      }

      override def next = {
        init
        if (!stillMore) sys.error("hit end of iterator")
        val toReturn = (
          ifConf.kExtract(recordReader.getCurrentKey),
          ifConf.vExtract(recordReader.getCurrentValue)
        )
        stillMore = recordReader.nextKeyValue
        toReturn
      }
    }
  }
}
