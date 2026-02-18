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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce.JobContext

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{
  CommitValidationException,
  HadoopMapReduceCommitProtocol,
  RuleFailure,
  SqlValidationRule,
  SqlValidationRuleJson
}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.SparkSession

/**
 * A [[HadoopMapReduceCommitProtocol]] subclass that runs user-defined SQL validation rules
 * against the staged output before committing the job.
 *
 * Rules are expressed as SQL `SELECT` assertions using a `failIf` convention: if the query
 * returns any rows, the rule fails. All rules are evaluated regardless of individual failures
 * (collect-all semantics). On failure the job is aborted and a [[CommitValidationException]]
 * is thrown listing every failed rule with sample offending rows.
 *
 * == Configuration ==
 *
 * Rule sources, in precedence order (first non-empty wins):
 *  1. Override `validationRules` in a subclass (Scala/Java engineers).
 *  2. `spark.io.hdfs.commit.validationRules` — inline JSON array of rule objects.
 *  3. `spark.io.hdfs.commit.validationRulesFile` — HDFS path to a JSON file of rule objects.
 *
 * Additional conf keys:
 *  - `spark.io.hdfs.commit.validationInputFormat` — format used to read the staging directory
 *    (default: `parquet`). Must match the format of the write job.
 *
 * == Usage ==
 *
 * Set the commit protocol class:
 * {{{
 *   spark.conf.set(
 *     "spark.sql.sources.commitProtocolClass",
 *     "org.apache.spark.sql.execution.datasources.HdfsValidatingCommitProtocol")
 * }}}
 *
 * Then write as usual:
 * {{{
 *   df.write.mode("overwrite").parquet("hdfs:///output/my-dataset")
 * }}}
 *
 * @param jobId                    the job's or stage's id
 * @param path                     the job's output path
 * @param dynamicPartitionOverwrite If true, use dynamic partition overwrite mode
 */
class HdfsValidatingCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends HadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable
    with Logging {

  // Conf key constants
  private val CONF_VALIDATION_RULES = "spark.io.hdfs.commit.validationRules"
  private val CONF_VALIDATION_RULES_FILE = "spark.io.hdfs.commit.validationRulesFile"
  private val CONF_VALIDATION_INPUT_FORMAT = "spark.io.hdfs.commit.validationInputFormat"
  private val DEFAULT_INPUT_FORMAT = "parquet"

  /**
   * Override this method in a subclass to supply validation rules programmatically.
   * When non-empty, this takes precedence over conf-based rule sources.
   */
  def validationRules: Seq[SqlValidationRule] = Seq.empty

  // -------------------------------------------------------------------------
  // Rule resolution
  // -------------------------------------------------------------------------

  private def resolveRules(spark: SparkSession): Seq[SqlValidationRule] = {
    // Tier 1: Scala override
    val overrideRules = validationRules
    if (overrideRules.nonEmpty) return overrideRules

    // Tier 2: inline JSON conf
    val inlineJson = spark.conf.getOption(CONF_VALIDATION_RULES).getOrElse("").trim
    if (inlineJson.nonEmpty) return SqlValidationRuleJson.parseRulesJson(inlineJson)

    // Tier 3: file-backed conf
    val filePath = spark.conf.getOption(CONF_VALIDATION_RULES_FILE).getOrElse("").trim
    if (filePath.nonEmpty) {
      return SqlValidationRuleJson.readRulesFile(
        filePath,
        spark.sparkContext.hadoopConfiguration)
    }

    Seq.empty
  }

  // -------------------------------------------------------------------------
  // Staged output registration
  // -------------------------------------------------------------------------

  private def registerStagedOutput(spark: SparkSession): Unit = {
    val format = spark.conf.getOption(CONF_VALIDATION_INPUT_FORMAT)
      .getOrElse(DEFAULT_INPUT_FORMAT)
    val stagingPath = stagingDir.toString
    try {
      spark.read
        .format(format)
        .option("recursiveFileLookup", "true")
        .load(stagingPath)
        .createOrReplaceTempView("__output__")
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to read staged output from '$stagingPath' as format '$format'. " +
            s"If your write job uses a different format, set " +
            s"'$CONF_VALIDATION_INPUT_FORMAT' accordingly. Underlying error: ${e.getMessage}",
          e)
    }
  }

  // -------------------------------------------------------------------------
  // Rule classification helpers
  // -------------------------------------------------------------------------

  /**
   * Returns true if the SQL is an aggregate (column-level) query.
   * Such queries must NOT have `LIMIT 10` appended, because:
   *  - `GROUP BY` queries return one row per group — truncating with LIMIT would hide
   *    offending groups and give a misleading picture.
   *  - Scalar aggregates (`COUNT(*) < 1000`) return a single row that is already compact.
   *
   * Classification signals (any one is sufficient):
   *  - Contains an aggregate function call: `COUNT(`, `SUM(`, `MIN(`, `MAX(`, `AVG(`,
   *    `STDDEV(`, `VARIANCE(`
   *  - Contains a `GROUP BY` clause
   *  - Contains a `HAVING` clause
   *
   * Row-level rules (e.g. `SELECT * FROM __output__ WHERE user_id IS NULL`) match none of
   * these signals and will have `LIMIT 10` appended automatically.
   *
   * '''Heuristic limitations''': This is a best-effort text classifier, not a full SQL parser.
   * Known edge cases:
   *  - A row-returning query that wraps an aggregate subquery
   *    (e.g. `SELECT * FROM t WHERE v > (SELECT MAX(x) FROM t2)`) will be classified as
   *    aggregate, suppressing the auto-LIMIT. Workaround: rewrite to avoid aggregate
   *    keywords in the outer query.
   */
  private[datasources] def isAggregate(sql: String): Boolean = {
    val upper = sql.trim.toUpperCase
    // Aggregate function call keywords
    val aggFnPattern = """(?i)\b(COUNT|SUM|MIN|MAX|AVG|STDDEV|VARIANCE)\s*\(""".r
    aggFnPattern.findFirstIn(upper).isDefined ||
      upper.contains("GROUP BY") ||
      upper.contains("HAVING")
  }

  /**
   * Appends `LIMIT 10` to row-returning queries that do not already contain a LIMIT clause
   * (case-insensitive, any form: `LIMIT 10`, `LIMIT ALL`, etc.).
   * Aggregate queries are returned unchanged.
   */
  private[datasources] def applyAutoLimit(sql: String): String = {
    if (isAggregate(sql)) return sql
    if (sql.trim.toUpperCase.matches("""(?s).*\bLIMIT\b.*""")) return sql
    sql.stripTrailing() + " LIMIT 10"
  }

  // -------------------------------------------------------------------------
  // Rule execution engine
  // -------------------------------------------------------------------------

  /**
   * Classify rules, fuse where possible, execute, and return all failures.
   */
  private def executeRules(
      spark: SparkSession,
      rules: Seq[SqlValidationRule]): Seq[RuleFailure] = {

    val aggRules = rules.filter(r => isAggregate(r.failIf))
    val rowRules = rules.filter(r => !isAggregate(r.failIf))

    val failures = scala.collection.mutable.ArrayBuffer[RuleFailure]()

    // --- aggregate fusion ---
    // Wrap each aggregate failIf as a subquery that returns a tagged row when the condition
    // is true (i.e., the rule fails).
    if (aggRules.nonEmpty) {
      val unionParts = aggRules.map { r =>
        val limitedSql = r.failIf.trim.stripSuffix(";")
        // Wrap: SELECT '<name>' AS __rule__, <agg-query result> — but we only care about
        // whether there is a row, so we select from the failIf as a subquery.
        s"SELECT '${escapeSingleQuote(r.name)}' AS __rule__ FROM ($limitedSql) __agg_check__"
      }
      val fusedSql = unionParts.mkString("\nUNION ALL\n")
      try {
        val resultDf = spark.sql(fusedSql)
        val resultRows = resultDf.collect()
        val failedAggNames = resultRows.map(_.getString(0)).toSet
        for (r <- aggRules if failedAggNames.contains(r.name)) {
          val sampleDf = spark.sql(r.failIf.trim.stripSuffix(";"))
          val sampleRows = sampleDf.collect().take(10).toSeq
          failures += RuleFailure(r.name, r.description, r.failIf, sampleRows)
        }
      } catch {
        case e: Exception =>
          // Fallback: execute individually
          logWarning(s"Aggregate rule fusion failed; falling back to individual execution. " +
            s"Error: ${e.getMessage}")
          for (r <- aggRules) {
            failures ++= executeRuleIndividually(spark, r)
          }
      }
    }

    // --- row-returning fusion via UNION ALL ---
    if (rowRules.nonEmpty) {
      val taggedParts = rowRules.map { r =>
        val limitedSql = applyAutoLimit(r.failIf).trim.stripSuffix(";")
        s"SELECT '${escapeSingleQuote(r.name)}' AS __rule__, * FROM ($limitedSql) __row_check__"
      }
      val fusedSql = taggedParts.mkString("\nUNION ALL\n")
      try {
        val resultDf = spark.sql(fusedSql)
        val allRows = resultDf.collect()
        // Group rows by rule name (first column)
        val byRule = allRows.groupBy(row => row.getString(0))
        for (r <- rowRules) {
          val ruleRows = byRule.getOrElse(r.name, Array.empty)
          if (ruleRows.nonEmpty) {
            // Drop the __rule__ tag column to get the original columns
            val sampleRows = ruleRows.take(10).map { row =>
              val values = (1 until row.length).map(row.get)
              org.apache.spark.sql.Row.fromSeq(values)
            }.toSeq
            failures += RuleFailure(r.name, r.description,
              applyAutoLimit(r.failIf), sampleRows)
          }
        }
      } catch {
        case e: Exception =>
          logWarning(s"Row-returning rule fusion failed; falling back to individual execution. " +
            s"Error: ${e.getMessage}")
          for (r <- rowRules) {
            failures ++= executeRuleIndividually(spark, r)
          }
      }
    }

    failures.toSeq
  }

  /** Execute a single rule individually (fallback path). */
  private def executeRuleIndividually(
      spark: SparkSession,
      rule: SqlValidationRule): Seq[RuleFailure] = {
    logWarning(s"Executing validation rule '${rule.name}' individually (could not be fused).")
    val sql = applyAutoLimit(rule.failIf).trim.stripSuffix(";")
    val rows = spark.sql(sql).collect()
    if (rows.nonEmpty) {
      Seq(RuleFailure(rule.name, rule.description, sql, rows.take(10).toSeq))
    } else {
      Seq.empty
    }
  }

  private def escapeSingleQuote(s: String): String = s.replace("'", "\\'")

  // -------------------------------------------------------------------------
  // commitJob override
  // -------------------------------------------------------------------------

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val sparkOpt = SparkSession.getActiveSession
    if (sparkOpt.isEmpty) {
      throw new IllegalStateException(
        "HdfsValidatingCommitProtocol requires an active SparkSession to execute validation " +
          "rules. Ensure SparkSession is running on the driver when commitJob is called.")
    }
    val spark = sparkOpt.get

    val rules = resolveRules(spark)
    if (rules.isEmpty) {
      // No rules configured — commit normally.
      super.commitJob(jobContext, taskCommits)
      return
    }

    registerStagedOutput(spark)
    val failures = executeRules(spark, rules)

    if (failures.nonEmpty) {
      super.abortJob(jobContext)
      throw new CommitValidationException(failures, rules.map(_.name))
    } else {
      super.commitJob(jobContext, taskCommits)
    }
  }
}
