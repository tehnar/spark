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
package org.apache.spark.sql.connect

import org.apache.spark.TaskContext
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.functions.{col, udf}

/**
 * End-to-end tests for TaskContext usage in Spark Connect UDFs.
 *
 * These tests verify that:
 * 1. UDFs compiled against the connect-udf TaskContext API work correctly
 * 2. The TaskContext from connect-udf is binary compatible with core TaskContext
 * 3. TaskContext methods can be called inside UDFs executed via Spark Connect
 */
class TaskContextE2ETestSuite
    extends ConnectFunSuite
    with RemoteSparkSession
    with SQLHelper {

  test("TaskContext.get() returns a valid context in UDF") {
    val getTaskInfo = udf((_: Long) => {
      val tc = TaskContext.get()
      assert(tc != null, "TaskContext should not be null inside a task")
      s"partition=${tc.partitionId()},attempt=${tc.attemptNumber()}"
    })

    // Use range with explicit partitions to ensure multi-partition execution
    val result = spark.range(0, 100, 1, 4)
      .select(getTaskInfo(col("id")))
      .collect()
      .map(_.getString(0))

    // Verify that we got task info from different partitions
    assert(result.nonEmpty)
    assert(result.exists(_.contains("partition=0")))
    assert(result.exists(_.contains("partition=1")))
  }

  test("TaskContext.stageId() and stageAttemptNumber() work in UDF") {
    val getStageInfo = udf((_: Long) => {
      val tc = TaskContext.get()
      s"stage=${tc.stageId()},stageAttempt=${tc.stageAttemptNumber()}"
    })

    val result = spark.range(0, 50, 1, 2)
      .withColumn("stage_info", getStageInfo(col("id")))
      .select("stage_info")
      .collect()
      .map(_.getString(0))

    // All tasks in the same stage should have the same stageId
    assert(result.nonEmpty, "Should have results")
    val stageIds = result.map(_.split(",")(0)).distinct
    assert(stageIds.length == 1, "All tasks should be in the same stage")
    assert(result.head.contains("stageAttempt=0"), "First attempt should be 0")
  }

  test("TaskContext.getLocalProperty()") {

    val getProperty = (key: String) => udf((_: Long) => {
      val tc = TaskContext.get()
      val prop = tc.getLocalProperty(key)
      if (prop == null) s"null" else prop
    })
    spark.addTag("myTag")

    val getNonExistentProp = getProperty("nonexistent.key")
    val getJobTag = getProperty("spark.job.tags")


    val nullResult = spark.range(0, 50, 1, 2)
      .select(getNonExistentProp(col("id")))
      .collect()
      .map(_.getString(0))

    val tagResult = spark.range(0, 50, 1, 2)
      .select(getJobTag(col("id")))
      .collect()
      .map(_.getString(0))

    assert(nullResult.forall(_.startsWith("null")),
      "Should return null for nonexistent property")
//    throw new RuntimeException(tagResult.mkStrin  g("Array(", ", ", ")"))
    assert(tagResult.forall(_.contains("myTag")),
      "Job tag property should be present")
  }

  test("TaskContext.addTaskCompletionListener works in UDF") {
    val addListener = udf((_: Long) => {
      val tc = TaskContext.get()
      tc.addTaskCompletionListener(
        new org.apache.spark.util.TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
          }
        })
      "ok"
    })

    val result = spark.range(0, 1, 1, 1)
      .select(addListener(col("id")))
      .collect()

    assert(result.forall(_.getString(0).startsWith("ok")))
  }

  test("TaskContext.resources() returns non-null map in UDF") {
    val getResourcesSize = udf((_: Long) => {
      val tc = TaskContext.get()
      val resources = tc.resources()
      assert(resources != null, "Resources map should not be null")
      resources.size
    })

    val result = spark.range(0, 50, 1, 2)
      .select(getResourcesSize(col("id")))
      .collect()
      .map(_.getInt(0))

    // Resources map should not be null (though it may be empty)
    result.foreach { size =>
      assert(size >= 0, "Resources size should be non-negative")
    }
  }
}
