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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}

class TestSourceMicroBatchStream(numPartitions: Int, includeTimestamp: Boolean,
                                 startOffset: Int, numMessagesPerBatch: Int)
  extends MicroBatchStream with Logging {

  var currentOffset: Int = startOffset
  /**
   * Returns the most recent offset available.
   */
  override def latestOffset(): Offset = {
    logDebug(s"<<<<<<< latestOffset - currentOffset: $currentOffset")
    currentOffset += numMessagesPerBatch
    new TestSourceOffset(currentOffset)
  }

  /**
   * Returns a list of InputPartition input partitions given the start and end offsets. Each
   * InputPartition represents a data split that can be processed by one Spark task. The
   * number of input partitions returned here is the same as the number of RDD partitions this scan
   * outputs.
   * <p>
   * If the Scan supports filter pushdown, this stream is likely configured with a filter
   * and is responsible for creating splits for that filter, which is not a full scan.
   * </p>
   * <p>
   * This method will be called multiple times, to launch one Spark job for each micro-batch in this
   * data stream.
   * </p>
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    logDebug(s"<<<<<<< planInputPartitions - start: $start end: $end")
    val partitions = new Array[InputPartition](numPartitions)
    for (i <- 0 until numPartitions) {
      partitions(i) = new TestSourceInputPartition(i,
        start.asInstanceOf[TestSourceOffset],
        end.asInstanceOf[TestSourceOffset])
    }
    partitions
  }

  /**
   * Returns a factory to create a PartitionReader for each InputPartition.
   */
  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("<<<<<<< createReaderFactory")
    new TestSourceReaderFactory(includeTimestamp)
  }

  /**
   * Returns the initial offset for a streaming query to start reading from. Note that the
   * streaming data source should not assume that it will start reading from its initial offset:
   * if Spark is restarting an existing query, it will restart from the check-pointed offset rather
   * than the initial one.
   */
  override def initialOffset(): Offset = {
    logDebug("<<<<<<< initialOffset")
    new TestSourceOffset(startOffset)
  }

  /**
   * Deserialize a JSON string into an Offset of the implementation-defined offset type.
   *
   * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
   */
  override def deserializeOffset(json: String): Offset = {
    logDebug(s"<<<<<<< deserializeOffset - json: $json")
    TestSourceOffset.deserialize(json)
  }

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  override def commit(end: Offset): Unit = {
    logDebug(s"<<<<<<< commit - end: $end")
  }

  /**
   * Stop this source and free any resources it has allocated.
   */
  override def stop(): Unit = {
    logDebug("<<<<<<< stop")
  }
}
