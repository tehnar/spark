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
package org.apache.spark.sql.connect.udf

import java.io.{File, Writer}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import scala.collection.mutable.{Set => MutableSet}

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.lib.MiMaLib

import org.apache.spark.SparkBuildInfo.spark_version

/**
 * A tool for checking the binary compatibility of the connect-udf TaskContext API against
 * the core TaskContext API using MiMa. This ensures that user code compiled against the
 * connect-udf TaskContext can run with the core TaskContext in the classpath.
 * We did not write this check using a SBT build rule as the rule cannot provide
 * the same level of freedom as a test. With a test we can:
 *   1. Specify any two jars to run the compatibility check.
 *   2. Easily make the test automatically pick up all new methods added while the client is being
 *      built.
 *
 * This check can be run by executing: `dev/connect-udf-mima-check`
 */
// scalastyle:off println
object CheckTaskContextCompatibility {

  private lazy val sparkHome: String = {
    if (!sys.env.contains("SPARK_HOME")) {
      throw new IllegalArgumentException("SPARK_HOME is not set.")
    }
    sys.env("SPARK_HOME")
  }

  private val scalaVersion = scala.util.Properties.versionNumberString match {
    case v if v.startsWith("2.13.") => "2.13"
    case v if v.startsWith("2.12.") => "2.12"
    case v => throw new IllegalArgumentException(s"Unsupported Scala version: $v")
  }

  private val coreJar = {
    val path = Paths.get(
      sparkHome,
      "core",
      "target",
      s"scala-$scalaVersion",
      s"spark-core_${scalaVersion}-$spark_version.jar")
    assert(Files.exists(path), s"$path does not exist")
    path.toFile
  }

  private val connectUdfJar = {
    val path = Paths.get(
      sparkHome,
      "sql",
      "connect",
      "udf",
      "target",
      s"scala-$scalaVersion",
      s"spark-connect-udf_${scalaVersion}-$spark_version.jar")
    assert(Files.exists(path), s"$path does not exist")
    path.toFile
  }

  def main(args: Array[String]): Unit = {
    var resultWriter: Writer = null
    try {
      resultWriter = Files.newBufferedWriter(
        Paths.get(s"$sparkHome/.connect-udf-mima-check-result"),
        StandardCharsets.UTF_8)

      // Check that connect-udf TaskContext is compatible with core TaskContext
      // This ensures code compiled against connect-udf can run with core
      val problems = checkTaskContextCompatibility(connectUdfJar, coreJar)

      if (problems.nonEmpty) {
        resultWriter.write(
          s"ERROR: Binary compatibility check failed between:\n")
        resultWriter.write(
          s"  Connect UDF jar: $connectUdfJar\n")
        resultWriter.write(
          s"  Core jar: $coreJar\n\n")
        resultWriter.write(
          s"TaskContext in connect-udf must be binary compatible with TaskContext in core.\n")
        resultWriter.write(
          s"Problems found:\n\n")

        val problemDescriptions = problems.map { p =>
          s"  ${p.getClass.getSimpleName}: ${p.description("connect-udf")}"
        }
        resultWriter.write(problemDescriptions.mkString("\n"))
        resultWriter.write("\n\n")
      }
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        if (resultWriter != null) {
          resultWriter.write(s"ERROR: ${e.getMessage}\n")
        }
    } finally {
      if (resultWriter != null) {
        resultWriter.close()
      }
    }
  }

  /**
   * MiMa takes a new jar and an old jar as inputs and then reports all incompatibilities found in
   * the new jar. The incompatibility result is then filtered using include and exclude rules.
   * Include rules are first applied to find all client classes that need to be checked. Then
   * exclude rules are applied to filter out all unsupported methods in the client classes.
   */
  private def checkTaskContextCompatibility(
      connectUdfJar: File,
      coreJar: File): List[Problem] = {

    // Include TaskContext class, companion object, and related listener traits
    // Use wildcards to match all members (methods, fields) of these classes
    val includedRules = Seq(
      IncludeByName("org.apache.spark.TaskContext.*"),
      IncludeByName("org.apache.spark.TaskContext$.*"),
      IncludeByName("org.apache.spark.resource.ResourceInformation.*"),
      IncludeByName("org.apache.spark.util.TaskCompletionListener"),
      IncludeByName("org.apache.spark.util.TaskFailureListener")
    )

    val excludeRules = Seq(
      // Developer APIs, not exposed to Spark Connect users
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.TaskContext.taskMetrics"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.TaskContext.getMetricsSources")
    )

    checkMiMaCompatibility(connectUdfJar, coreJar, includedRules, excludeRules)
  }

  /**
   * MiMa takes a new jar and an old jar as inputs and then reports all incompatibilities found in
   * the new jar. The incompatibility result is then filtered using include and exclude rules.
   * Include rules are first applied to find all client classes that need to be checked. Then
   * exclude rules are applied to filter out all unsupported methods in the client classes.
   */
  private def checkMiMaCompatibility(
      newJar: File,
      oldJar: File,
      includedRules: Seq[IncludeByName],
      excludeRules: Seq[ProblemFilter]): List[Problem] = {

    val mima = new MiMaLib(Seq(newJar, oldJar))
    val allProblems = mima.collectProblems(oldJar, newJar, List.empty)

    val effectiveExcludeRules = MutableSet.empty[ProblemFilter]
    val problems = allProblems
      .filter { p =>
        includedRules.exists(rule => rule(p))
      }
      .filter { p =>
        excludeRules.forall { rule =>
          val passedRule = rule(p)
          if (!passedRule) {
            effectiveExcludeRules += rule
          }
          passedRule
        }
      }

    // Warn about unused exclude rules
    excludeRules.filterNot(effectiveExcludeRules.contains).foreach { rule =>
      println(s"Warning: $rule did not filter out any problems (may be unnecessary).")
    }

    problems
  }

  private case class IncludeByName(name: String) extends ProblemFilter {
    private[this] val pattern =
      Pattern.compile(name.split("\\*", -1).map(Pattern.quote).mkString(".*"))

    override def apply(problem: Problem): Boolean = {
      pattern.matcher(problem.matchName.getOrElse("")).matches
    }
  }
}
