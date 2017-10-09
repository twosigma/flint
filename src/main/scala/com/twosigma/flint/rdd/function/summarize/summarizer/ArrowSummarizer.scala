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

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import com.twosigma.flint.arrow.{ ArrowFieldWriter, ArrowPayload, ArrowUtils, ArrowWriter }
import org.apache.arrow.memory.{ BufferAllocator, RootAllocator }
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.file.ArrowFileWriter
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
  var allocator: BufferAllocator,
  var root: VectorSchemaRoot,
  var arrowWriter: ArrowWriter
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
    new ArrowSummarizerState(false, null, null, null)
  }

  private def init(u: ArrowSummarizerState): Unit = {
    if (!u.initialized) {
      val arrowSchema = ArrowUtils.toArrowSchema(schema)
      val allocator = new RootAllocator(Int.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = ArrowWriter.create(root)

      u.initialized = true
      u.allocator = allocator
      u.root = root
      u.arrowWriter = arrowWriter
    }
  }

  override def add(u: ArrowSummarizerState, row: InternalRow): ArrowSummarizerState = {
    if (!u.initialized) {
      init(u)
    }
    u.arrowWriter.write(row)
    u
  }

  override def merge(
    u1: ArrowSummarizerState,
    u2: ArrowSummarizerState
  ): ArrowSummarizerState = throw new UnsupportedOperationException()

  // This can only be called once
  override def render(u: ArrowSummarizerState): Array[Byte] = {
    if (u.initialized) {
      val out = new ByteArrayOutputStream()
      val writer = new ArrowFileWriter(u.root, null, Channels.newChannel(out))

      u.arrowWriter.finish()
      writer.writeBatch()

      writer.close()
      u.root.close()
      u.allocator.close()

      out.toByteArray
    } else {
      Array.empty
    }
  }

  override def close(u: ArrowSummarizerState): Unit = {
    if (u.initialized) {
      u.arrowWriter.reset()
      u.root.close()
      u.allocator.close()
    }
  }
}
