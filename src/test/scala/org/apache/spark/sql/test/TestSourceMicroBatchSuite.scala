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

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest, Trigger}

class TestSourceMicroBatchSuite extends StreamTest with SharedSparkSession {

  test("basic usage") {

    val reader = spark
      .readStream
      .format("test")
      .option("includeTimestamp", "true")
      .load()

    var index: Int = 0
    def startTriggerAvailableNowQuery(): StreamingQuery = {
      reader.writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .foreachBatch((ds: Dataset[Row], _: Long) => {
          index += 1
          ds.foreach((r: Row) => {
            println("row: " + r.toString)
          })
        })
        .start()
    }

    val query = startTriggerAvailableNowQuery()
    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
    } finally {
      query.stop()
    }
  }

}
