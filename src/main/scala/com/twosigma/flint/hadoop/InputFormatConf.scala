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
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.{ LongWritable, Text, Writable }
import org.apache.hadoop.mapreduce.{ InputFormat, InputSplit, Job, RecordReader }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat, FileSplit, TextInputFormat }

import scala.collection.immutable

trait InputFormatConf[K, V] extends Serializable {
  type IF <: InputFormat[K, V]
  type Split <: InputSplit with Writable

  type KExtract <: Extract[K]
  type VExtract <: Extract[V]

  def kExtract: KExtract
  def vExtract: VExtract

  def makeInputFormat(): IF

  // I'm unsure if we should WriSer them for them
  def makeSplits(hadoopConf: Configuration): IndexedSeq[WriSer[Split]]

  // TODO do we want to require typing of the RecordReader as well?
  final def createRecordReader(hadoopConf: Configuration, split: Split,
    inputFormat: IF = makeInputFormat()): RecordReader[K, V] = {
    val tac = ConfOnlyTAC(hadoopConf)
    val recordReader = inputFormat.createRecordReader(split, tac)
    recordReader.initialize(split, tac)
    recordReader
  }
}

case class TextInputFormatConf(file: String, partitions: Int)
  extends InputFormatConf[LongWritable, Text] {
  type IF = TextInputFormat
  type Split = FileSplit

  // TODO now that we figured out what's up, see if we can't eliminate the need for this...
  val internalK = Extract.unit[LongWritable]
  val internalV = Extract.text

  type KExtract = internalK.type
  type VExtract = internalV.type

  override val kExtract: KExtract = internalK
  override val vExtract: VExtract = internalV

  def makeInputFormat() = new TextInputFormat()
  def makeSplits(hadoopConf: Configuration): immutable.IndexedSeq[WriSer[FileSplit]] = {
    val job = Job.getInstance(hadoopConf)
    FileInputFormat.setInputPaths(job, file)
    val path = new Path(file)
    val len = FileSystem.get(hadoopConf).listStatus(path).head.getLen
    val size_per = math.round(len / partitions.toDouble)

    ((0 until partitions - 1).map { p =>
      new FileSplit(path, size_per * p, size_per, null)
    } :+ {
      val fin = size_per * (partitions - 1)
      new FileSplit(path, fin, len - fin, null)
    }).map(WriSer(_))
  }
}

// TODO do we really get much from having this as its own class? consider just making a def csv method in TextInputFormatConf
object CSVInputFormatConf {
  def apply[V](ifc: InputFormatConf[LongWritable, V] { type Split = FileSplit }): InputFormatConf[LongWritable, V] {
    type IF = ifc.IF
    type Split = ifc.Split
    type KExtract = ifc.KExtract
    type VExtract = ifc.VExtract
  } = new InputFormatConf[LongWritable, V] {
    type IF = ifc.IF
    type Split = ifc.Split
    type KExtract = ifc.KExtract
    type VExtract = ifc.VExtract

    override val kExtract: KExtract = ifc.kExtract
    override val vExtract: VExtract = ifc.vExtract

    override def makeInputFormat() = ifc.makeInputFormat()
    override def makeSplits(hadoopConf: Configuration) = {
      val splits = ifc.makeSplits(hadoopConf)
      splits.headOption.fold(IndexedSeq.empty[WriSer[Split]]) {
        case WriSer(head) =>
          val rr = createRecordReader(hadoopConf, head)
          require(rr.nextKeyValue, "csv has no header, first line was empty")
          val afterHeader = rr.getCurrentKey.get
          require(rr.nextKeyValue, "first split is empty")
          WriSer(new FileSplit(head.getPath, afterHeader, head.getLength - afterHeader, null)) +:
            splits.tail
      }
    }
  }
}
