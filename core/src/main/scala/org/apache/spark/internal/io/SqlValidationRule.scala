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

package org.apache.spark.internal.io

import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row

/**
 * A SQL-based validation rule for use with [[HdfsValidatingCommitProtocol]].
 *
 * The `failIf` query is executed against the staged output (registered as the
 * `__output__` temporary view). If it returns any rows the rule fails and the
 * job is aborted.
 *
 * @param name        Unique identifier for the rule (used in error reports).
 * @param failIf      SQL query: non-empty result → rule fails.
 * @param description Human-readable label included in [[CommitValidationException]] messages.
 */
case class SqlValidationRule(name: String, failIf: String, description: String)
  extends Serializable

/**
 * Captures the outcome of a single failed validation rule.
 *
 * @param name        Rule name.
 * @param description Rule description.
 * @param failIfQuery The SQL query that was executed (may have had LIMIT appended).
 * @param sampleRows  Up to 10 rows returned by the `failIf` query.
 */
case class RuleFailure(
    name: String,
    description: String,
    failIfQuery: String,
    sampleRows: Seq[Row])

/**
 * Thrown by [[HdfsValidatingCommitProtocol]] when one or more validation rules fail.
 * The exception message summarises every failure with sample offending rows and lists
 * passing rules as well, so the full picture is visible in one place.
 *
 * @param failures     All rules that returned rows from their `failIf` queries.
 * @param allRuleNames Names of every rule that was evaluated (passed and failed).
 */
class CommitValidationException(failures: Seq[RuleFailure], allRuleNames: Seq[String])
  extends RuntimeException(CommitValidationException.buildMessage(failures, allRuleNames))

private object CommitValidationException {
  def buildMessage(failures: Seq[RuleFailure], allRuleNames: Seq[String]): String = {
    val total = allRuleNames.size
    val nFailed = failures.size
    val failedNames = failures.map(_.name).toSet

    val sb = new StringBuilder
    sb.append(s"$nFailed of $total validation rules failed\n")

    for (f <- failures) {
      sb.append(s"\n  FAILED  ${f.name} — \"${f.description}\"\n")
      sb.append(s"          Query: ${f.failIfQuery}\n")
      if (f.sampleRows.nonEmpty) {
        val rowCount = f.sampleRows.size
        sb.append(s"          Sample ($rowCount row(s) returned):\n")
        for (row <- f.sampleRows) {
          sb.append(s"          ${row.mkString("[", ", ", "]")}\n")
        }
      }
    }

    for (name <- allRuleNames if !failedNames.contains(name)) {
      sb.append(s"\n  PASSED  $name\n")
    }

    sb.toString()
  }
}

// ---------------------------------------------------------------------------
// JSON codec helpers (package-private, used by HdfsValidatingCommitProtocol)
// ---------------------------------------------------------------------------

private[io] object SqlValidationRuleJson {

  private val mapper = new ObjectMapper()

  /** Serialise a single rule to a JSON object string. */
  def toJson(rule: SqlValidationRule): String =
    mapper.writeValueAsString(
      mapper.createObjectNode()
        .put("name", rule.name)
        .put("failIf", rule.failIf)
        .put("description", rule.description))

  /** Serialise a sequence of rules to a JSON array string. */
  def toJsonArray(rules: Seq[SqlValidationRule]): String = {
    val arrayNode = mapper.createArrayNode()
    rules.foreach { r =>
      arrayNode.add(
        mapper.createObjectNode()
          .put("name", r.name)
          .put("failIf", r.failIf)
          .put("description", r.description))
    }
    mapper.writeValueAsString(arrayNode)
  }

  /**
   * Parse a JSON array string into a sequence of [[SqlValidationRule]]s.
   * Throws [[IllegalArgumentException]] on malformed input.
   */
  def parseRulesJson(json: String): Seq[SqlValidationRule] = {
    val root: JsonNode =
      try {
        mapper.readTree(json)
      } catch {
        case e: JsonParseException =>
          throw new IllegalArgumentException(
            s"Failed to parse SqlValidationRule JSON: ${e.getMessage}", e)
      }

    if (!root.isArray) {
      throw new IllegalArgumentException(
        s"Expected a JSON array of SqlValidationRule objects, got: $json")
    }

    root.elements().asScala.toSeq.map { node =>
      def str(key: String): String = {
        val field = node.get(key)
        if (field == null || !field.isTextual) {
          throw new IllegalArgumentException(
            s"SqlValidationRule JSON missing or non-string field '$key' in: $node")
        }
        field.asText()
      }
      SqlValidationRule(
        name = str("name"),
        failIf = str("failIf"),
        description = str("description"))
    }
  }

  /**
   * Read a JSON file from HDFS (or any Hadoop-compatible filesystem) and parse it as a
   * sequence of [[SqlValidationRule]]s.
   */
  def readRulesFile(path: String, hadoopConf: Configuration): Seq[SqlValidationRule] = {
    val p = new Path(path)
    val fs = p.getFileSystem(hadoopConf)
    val is = fs.open(p)
    val json = try {
      scala.io.Source.fromInputStream(is, StandardCharsets.UTF_8.name()).mkString
    } finally {
      is.close()
    }
    parseRulesJson(json)
  }
}
