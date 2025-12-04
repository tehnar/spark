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

package org.apache.spark

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.annotation.{Evolving, Since}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}


/**
 * A stub that makes TaskContext APIs available to UDFs defined in Spark Connect. It is only
 * available in the Client classpath and should not be used outside of UDF execution.
 * On the Server side, TaskContext from Spark Core is used instead.
 */
object TaskContext {
  private def unsupportedException(): SparkUnsupportedOperationException =
    new SparkUnsupportedOperationException(
      "UNSUPPORTED_CONNECT_FEATURE.TASK_CONTEXT",
      Map.empty[String, String])

  /**
   * Return the currently active TaskContext. This can be called inside of
   * user functions to access contextual information about running tasks.
   */
  def get(): TaskContext = throw unsupportedException()

  /**
   * Returns the partition id of currently active TaskContext. It will return 0
   * if there is no active TaskContext for cases like local execution.
   */
  def getPartitionId(): Int = throw unsupportedException()

  def withTaskContext[T](context: TaskContext)(task: => T): T = throw unsupportedException()

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  /**
   * Set the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = throw unsupportedException()

  /**
   * Unset the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def unset(): Unit = throw unsupportedException()
}


/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task, use:
 * {{{
 *   org.apache.spark.TaskContext.get()
 * }}}
 *
 * This duplicates Spark Core TaskContext public APIs to make them available to
 * UDFs executed via Spark Connect in compile time. Spark Core TaskContext is used
 * during execution time. These two classes should be binary compatible.
 */
abstract class TaskContext private[spark] () extends Serializable {
  /**
   * Returns true if the task has completed.
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has failed.
   */
  def isFailed(): Boolean

  /**
   * Returns true if the task has been killed.
   */
  def isInterrupted(): Boolean

  /**
   * Adds a (Java friendly) listener to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation. Adding a listener
   * to an already completed task will result in that listener being called immediately.
   *
   * Two listeners registered in the same thread will be invoked in reverse order of registration if
   * the task completes after both are registered. There are no ordering guarantees for listeners
   * registered in different threads, or for listeners registered after the task completes.
   * Listeners are guaranteed to execute sequentially.
   *
   * An example use is for HadoopRDD to register a callback to close the input stream.
   *
   * Exceptions thrown by the listener will result in failure of the task.
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   * Adds a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation. Adding a listener
   * to an already completed task will result in that listener being called immediately.
   *
   * An example use is for HadoopRDD to register a callback to close the input stream.
   *
   * Exceptions thrown by the listener will result in failure of the task.
   */
  def addTaskCompletionListener[U](f: (TaskContext) => U): TaskContext = {
    // Note that due to this scala bug: https://github.com/scala/bug/issues/11016, we need to make
    // this function polymorphic for every scala version >= 2.12, otherwise an overloaded method
    // resolution error occurs at compile time.
    addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    })
  }

  /**
   * Adds a listener to be executed on task failure (which includes completion listener failure, if
   * the task body did not already fail). Adding a listener to an already failed task will result in
   * that listener being called immediately.
   *
   * Note: Prior to Spark 3.4.0, failure listeners were only invoked if the main task body failed.
   */
  def addTaskFailureListener(listener: TaskFailureListener): TaskContext

  /**
   * Adds a listener to be executed on task failure (which includes completion listener failure, if
   * the task body did not already fail). Adding a listener to an already failed task will result in
   * that listener being called immediately.
   *
   * Note: Prior to Spark 3.4.0, failure listeners were only invoked if the main task body failed.
   */
  def addTaskFailureListener(f: (TaskContext, Throwable) => Unit): TaskContext = {
    addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = f(context, error)
    })
  }

  /**
   * The ID of the stage that this task belong to.
   */
  def stageId(): Int

  /**
   * How many times the stage that this task belongs to has been attempted. The first stage attempt
   * will be assigned stageAttemptNumber = 0, and subsequent attempts will have increasing attempt
   * numbers.
   */
  def stageAttemptNumber(): Int

  /**
   * The ID of the RDD partition that is computed by this task.
   */
  def partitionId(): Int

  /**
   * Total number of partitions in the stage that this task belongs to.
   */
  def numPartitions(): Int

  /**
   * How many times this task has been attempted.  The first task attempt will be assigned
   * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
   */
  def attemptNumber(): Int

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
   */
  def taskAttemptId(): Long

  /**
   * Get a local property set upstream in the driver, or null if it is missing. See also
   * `org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String

  /**
   * CPUs allocated to the task.
   */
  @Since("3.3.0")
  def cpus(): Int

  /**
   * Resources allocated to the task. The key is the resource name and the value is information
   * about the resource. Please refer to [[org.apache.spark.resource.ResourceInformation]] for
   * specifics.
   */
  @Evolving
  def resources(): Map[String, ResourceInformation]

  /**
   * (java-specific) Resources allocated to the task. The key is the resource name and the value
   * is information about the resource. Please refer to
   * [[org.apache.spark.resource.ResourceInformation]] for specifics.
   */
  @Evolving
  def resourcesJMap(): java.util.Map[String, ResourceInformation]
}
