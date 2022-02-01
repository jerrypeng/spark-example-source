/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.test.TestSourceOffset.deserialize

class TestSourceOffset(val offset: Int) extends Offset with Serializable with Logging {

  /**
   * A JSON-serialized representation of an Offset that is
   * used for saving offsets to the offset log.
   * <p>
   * Note: We assume that equivalent/equal offsets serialize to
   * identical JSON strings.
   *
   * @return JSON string encoding
   */
  override def json(): String = {
    TestSourceOffset.mapper.writeValueAsString(Map("offset"->offset))
  }

  def apply(offset: SerializedOffset): TestSourceOffset = deserialize(offset.json)
}

object TestSourceOffset {
  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def deserialize(json: String): TestSourceOffset = {
    mapper.readValue(json, classOf[TestSourceOffset])
  }
}
