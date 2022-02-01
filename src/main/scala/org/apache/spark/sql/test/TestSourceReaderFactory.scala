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

import java.util.Calendar

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String

class TestSourceReaderFactory(includeTimestamp: Boolean)
  extends PartitionReaderFactory with Logging {
  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   * <p>
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    logDebug(s"<<<<<<< createReader - partition: $partition")
    new TestSourcePartitionReader(partition, includeTimestamp)
  }
}

class TestSourcePartitionReader(partition: InputPartition, includeTimestamp: Boolean)
  extends PartitionReader[InternalRow] with Logging{

  val testSourceInputPartition: TestSourceInputPartition
  = partition.asInstanceOf[TestSourceInputPartition]

  var current: Int = testSourceInputPartition.start.offset;
  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    logDebug("<<<<<<< next")
    testSourceInputPartition.end.offset > current
  }

  /**
   * Return the current record. This method should return same value until `next` is called.
   */
  override def get(): InternalRow = {
    logDebug("<<<<<<< get")
    var row: InternalRow = null
    if (includeTimestamp) {
      row = InternalRow(
        testSourceInputPartition.id,
        UTF8String.fromString("message-" + current.toString),
        DateTimeUtils.millisToMicros(Calendar.getInstance().getTimeInMillis))
    } else {
      row = InternalRow(
        testSourceInputPartition.id,
        UTF8String.fromString("message-" + current.toString))
    }
    current+=1
    row
  }

  override def close(): Unit = {
    logDebug("<<<<<<< close")
  }
}
