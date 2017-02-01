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

import java.io.{ DataInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream }
import java.io.IOException

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.Writable

// Note: we could make this implement InputSplit, but we do not because many input splits do a
// cast to their specific InputSplit, so we do not want to risk it. Further, this currently works
// for any Writable.
case class WriSer[T <: Writable: ClassTag](@transient var get: T) extends Serializable {
  def this() = this(null.asInstanceOf[T])

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(classTag[T])
    get.write(new DataOutputStream(out))
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  private def readObject(in: ObjectInputStream) {
    get = in.readObject.asInstanceOf[ClassTag[T]].runtimeClass.newInstance.asInstanceOf[T]
    get.readFields(new DataInputStream(in))
  }
}
