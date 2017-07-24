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

package com.twosigma.flint.rdd.function.summarize.summarizer

import com.twosigma.flint.arrow.{ ArrowPayload, ColumnWriter }
import org.apache.arrow.memory.{ BaseAllocator, RootAllocator }
import org.apache.arrow.vector.schema.ArrowRecordBatch
import org.apache.arrow.vector.stream.MessageSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * State is NOT serializable. This summarizer is not a distributed summarizer.
 */
class ArrowSummarizerState(
  var initialized: Boolean,
  var allocator: BaseAllocator,
  var vectorWriters: Array[ColumnWriter]
)

/**
 * Summarize rows in Arrow File Format.
 *
 * This summarizer differs from other summarizers:
 *
 * (1) It is not distributed, i.e., doesn't support merge operation
 * (2) It holds resources (offheap memory) and need to be manually freed, see close()
 *
 * This summarizer is only meant to be used in local mode, such as in summarizeCycles and summarizeWindows
 */
case class ArrowSummarizer(schema: StructType)
  extends Summarizer[InternalRow, ArrowSummarizerState, Array[Byte]] {
  private[this] val size = schema.size
  require(size > 0, "Cannot create summarizer with no input columns")

  // This function will allocate memory from the BufferAllocator to initialize arrow vectors.
  override def zero(): ArrowSummarizerState = {
    new ArrowSummarizerState(false, null, null)
  }

  private def init(u: ArrowSummarizerState): Unit = {
    if (!u.initialized) {
      var i = 0
      val writers = new Array[ColumnWriter](size)
      val allocator = new RootAllocator(Int.MaxValue)
      while (i < size) {
        val writer = ColumnWriter(schema.fields(i).dataType, i, allocator)
        writer.init()
        writers(i) = writer
        i += 1
      }

      u.initialized = true
      u.allocator = allocator
      u.vectorWriters = writers
    }
  }

  override def add(u: ArrowSummarizerState, row: InternalRow): ArrowSummarizerState = {
    if (!u.initialized) {
      init(u)
    }

    var i = 0
    while (i < size) {
      u.vectorWriters(i).write(row)
      i += 1
    }
    u
  }

  override def merge(
    u1: ArrowSummarizerState,
    u2: ArrowSummarizerState
  ): ArrowSummarizerState = throw new UnsupportedOperationException()

  // This can only be called once
  override def render(u: ArrowSummarizerState): Array[Byte] = {
    if (u.initialized) {
      val (fieldNodes, bufferArrays) = u.vectorWriters.map(_.finish()).unzip
      val buffers = bufferArrays.flatten

      val rowLength = if (fieldNodes.nonEmpty) fieldNodes.head.getLength else 0
      val recordBatch = new ArrowRecordBatch(
        rowLength,
        fieldNodes.toList.asJava, buffers.toList.asJava
      )
      buffers.foreach(_.release())

      val payload = ArrowPayload(recordBatch, schema, u.allocator)
      u.allocator.close()
      payload.asPythonSerializable
    } else {
      Array.empty
    }
  }

  override def close(u: ArrowSummarizerState): Unit = {
    if (u.initialized) {
      val (_, bufferArrays) = u.vectorWriters.map(_.finish()).unzip
      val buffers = bufferArrays.flatten
      buffers.foreach(_.release())
      u.allocator.close()
    }
  }
}
