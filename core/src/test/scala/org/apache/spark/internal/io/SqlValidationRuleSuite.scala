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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row

/**
 * Unit tests for SqlValidationRule, RuleFailure, CommitValidationException,
 * and SqlValidationRuleJson codec.
 */
class SqlValidationRuleSuite extends SparkFunSuite {

  // ---------------------------------------------------------------------------
  // 8.1 SqlValidationRule construction and Java serialization
  // ---------------------------------------------------------------------------

  test("SqlValidationRule exposes all three fields") {
    val rule = SqlValidationRule(
      name = "no_nulls",
      failIf = "SELECT * FROM __output__ WHERE user_id IS NULL",
      description = "user_id must never be null")
    assert(rule.name === "no_nulls")
    assert(rule.failIf === "SELECT * FROM __output__ WHERE user_id IS NULL")
    assert(rule.description === "user_id must never be null")
  }

  test("SqlValidationRule survives Java serialization round-trip") {
    val rule = SqlValidationRule("r1", "SELECT * FROM __output__ LIMIT 1", "test rule")
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(rule)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val restored = ois.readObject().asInstanceOf[SqlValidationRule]
    ois.close()

    assert(restored.name === rule.name)
    assert(restored.failIf === rule.failIf)
    assert(restored.description === rule.description)
  }

  // ---------------------------------------------------------------------------
  // 8.2 RuleFailure.sampleRows capping
  // ---------------------------------------------------------------------------

  test("RuleFailure.sampleRows holds at most 10 rows") {
    // Simulate 15 rows returned — only take(10) should be stored
    val rows = (1 to 15).map(i => Row(i)).take(10).toSeq
    val failure = RuleFailure("r", "desc", "SELECT ...", rows)
    assert(failure.sampleRows.size === 10)
  }

  test("RuleFailure.sampleRows holds a single aggregate row") {
    val rows = Seq(Row(342L))
    val failure = RuleFailure("min_rows", "At least 1000 rows", "SELECT COUNT(*) < 1000 FROM __output__", rows)
    assert(failure.sampleRows.size === 1)
    assert(failure.sampleRows.head.getLong(0) === 342L)
  }

  // ---------------------------------------------------------------------------
  // 8.3 CommitValidationException message format
  // ---------------------------------------------------------------------------

  test("CommitValidationException message contains ratio header") {
    val failures = Seq(
      RuleFailure("rule_a", "Rule A desc", "SELECT * FROM __output__ WHERE a IS NULL", Seq(Row(1, null))),
      RuleFailure("rule_b", "Rule B desc", "SELECT COUNT(*) < 5 FROM __output__", Seq(Row(3L)))
    )
    val allNames = Seq("rule_a", "rule_b", "rule_c", "rule_d")
    val ex = new CommitValidationException(failures, allNames)
    val msg = ex.getMessage
    assert(msg.contains("2 of 4 validation rules failed"),
      s"Expected ratio header in:\n$msg")
  }

  test("CommitValidationException message lists FAILED blocks with details") {
    val failures = Seq(
      RuleFailure("no_nulls", "no null user_ids", "SELECT * FROM __output__ WHERE user_id IS NULL",
        Seq(Row(99, null, "2024-01-01")))
    )
    val ex = new CommitValidationException(failures, Seq("no_nulls", "min_rows"))
    val msg = ex.getMessage
    assert(msg.contains("FAILED"), s"Missing FAILED in:\n$msg")
    assert(msg.contains("no_nulls"), s"Missing rule name in:\n$msg")
    assert(msg.contains("no null user_ids"), s"Missing description in:\n$msg")
    assert(msg.contains("user_id IS NULL"), s"Missing query in:\n$msg")
    assert(msg.contains("99"), s"Missing sample row data in:\n$msg")
  }

  test("CommitValidationException message lists PASSED rules") {
    val failures = Seq(
      RuleFailure("rule_a", "desc", "SELECT ...", Seq(Row(1)))
    )
    val allNames = Seq("rule_a", "rule_b")
    val ex = new CommitValidationException(failures, allNames)
    val msg = ex.getMessage
    assert(msg.contains("PASSED"), s"Missing PASSED in:\n$msg")
    assert(msg.contains("rule_b"), s"Missing passed rule name in:\n$msg")
  }

  // ---------------------------------------------------------------------------
  // 8.4 JSON codec round-trip
  // ---------------------------------------------------------------------------

  test("parseRulesJson round-trips a single rule") {
    val rule = SqlValidationRule("no_nulls", "SELECT * FROM __output__ WHERE id IS NULL", "no nulls")
    val json = SqlValidationRuleJson.toJsonArray(Seq(rule))
    val parsed = SqlValidationRuleJson.parseRulesJson(json)
    assert(parsed.size === 1)
    assert(parsed.head.name === rule.name)
    assert(parsed.head.failIf === rule.failIf)
    assert(parsed.head.description === rule.description)
  }

  test("parseRulesJson round-trips multiple rules preserving order") {
    val rules = Seq(
      SqlValidationRule("r1", "SELECT * FROM __output__ WHERE a IS NULL", "rule 1"),
      SqlValidationRule("r2", "SELECT COUNT(*) < 100 FROM __output__", "rule 2"),
      SqlValidationRule("r3", "SELECT * FROM __output__ WHERE b < 0", "rule 3")
    )
    val json = SqlValidationRuleJson.toJsonArray(rules)
    val parsed = SqlValidationRuleJson.parseRulesJson(json)
    assert(parsed.map(_.name) === Seq("r1", "r2", "r3"))
  }

  test("parseRulesJson handles special characters in fields") {
    val rule = SqlValidationRule(
      name = "quote_test",
      failIf = """SELECT * FROM __output__ WHERE name = "O'Brien" """,
      description = """Contains "quotes" and 'apostrophes'""")
    val json = SqlValidationRuleJson.toJsonArray(Seq(rule))
    val parsed = SqlValidationRuleJson.parseRulesJson(json)
    assert(parsed.head.description === rule.description)
  }

  test("parseRulesJson throws IllegalArgumentException on invalid JSON") {
    val ex = intercept[IllegalArgumentException] {
      SqlValidationRuleJson.parseRulesJson("not valid json")
    }
    assert(ex.getMessage.nonEmpty)
  }

  test("parseRulesJson throws on non-array JSON") {
    val ex = intercept[IllegalArgumentException] {
      SqlValidationRuleJson.parseRulesJson("""{"name":"x"}""")
    }
    assert(ex.getMessage.nonEmpty)
  }
}
