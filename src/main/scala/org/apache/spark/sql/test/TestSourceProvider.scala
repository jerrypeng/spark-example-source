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

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class TestSourceProvider extends SimpleTableProvider with DataSourceRegister with Logging {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    logDebug(s"<<<<<<< getTable - options: ${options.asCaseSensitiveMap()}")
    new TestTable(
      options.getInt("numPartitions", SparkSession.active.sparkContext.defaultParallelism),
      options.getBoolean("includeTimestamp", false),
      options.getInt("startOffset", 0),
      options.getInt("numMessagesPerBatch", 10)
    )
  }

  override def shortName(): String = "test"
}

class TestTable(numPartitions: Int, includeTimestamp: Boolean,
                startOffset: Int, numMessagesPerBatch: Int)
  extends Table with SupportsRead {

  override def name(): String = "test"

  override def toString(): String = {
    "test-table"
  }

  override def schema(): StructType = {
    if (includeTimestamp) {
      StructType(Array(
        StructField("partitionId", IntegerType),
        StructField("value", StringType),
        StructField("timestamp", TimestampType)))
    } else {
      StructType(Array(
        StructField("partitionId", IntegerType),
        StructField("value", StringType)))
    }
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new TestSourceMicroBatchStream(
        numPartitions, includeTimestamp, startOffset, numMessagesPerBatch)
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new IllegalStateException("not implemented yet!")
    }
  }
}
