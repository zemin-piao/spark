#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for ValidationRuleBuilder (tasks 8.11 and 8.12).

These tests do not require a live HDFS connection or a full Spark write.
They verify conf-key setting (8.11) and from_file loading behaviour (8.12)
using lightweight mocks.
"""

import json
import unittest

from unittest.mock import MagicMock, patch

from pyspark.sql.hdfs_validation import ValidationRuleBuilder

_PROTOCOL_CLASS = (
    "org.apache.spark.sql.execution.datasources.HdfsValidatingCommitProtocol"
)
_CONF_RULES = "spark.io.hdfs.commit.validationRules"
_CONF_RULES_FILE = "spark.io.hdfs.commit.validationRulesFile"
_CONF_COMMIT_PROTOCOL = "spark.sql.sources.commitProtocolClass"


class ValidationRuleBuilderTests(unittest.TestCase):

    # -----------------------------------------------------------------------
    # 8.11 apply() sets the correct conf keys
    # -----------------------------------------------------------------------

    def test_apply_sets_validation_rules_conf(self):
        """apply() serialises rules to JSON and sets spark.io.hdfs.commit.validationRules."""
        mock_spark = MagicMock()
        conf_store = {}
        mock_spark.conf.set.side_effect = lambda k, v: conf_store.__setitem__(k, v)

        builder = (
            ValidationRuleBuilder()
            .fail_if(
                "SELECT * FROM __output__ WHERE id IS NULL",
                name="no_nulls",
                description="no null ids",
            )
            .fail_if(
                "SELECT COUNT(*) < 1000 FROM __output__",
                name="min_rows",
                description="at least 1000 rows",
            )
        )
        builder.apply(mock_spark)

        self.assertIn(_CONF_RULES, conf_store)
        self.assertIn(_CONF_COMMIT_PROTOCOL, conf_store)
        self.assertEqual(conf_store[_CONF_COMMIT_PROTOCOL], _PROTOCOL_CLASS)

        rules = json.loads(conf_store[_CONF_RULES])
        self.assertEqual(len(rules), 2)
        self.assertEqual(rules[0]["name"], "no_nulls")
        self.assertEqual(rules[1]["name"], "min_rows")

    def test_apply_sets_commit_protocol_class(self):
        """apply() sets spark.sql.sources.commitProtocolClass to HdfsValidatingCommitProtocol."""
        mock_spark = MagicMock()
        conf_store = {}
        mock_spark.conf.set.side_effect = lambda k, v: conf_store.__setitem__(k, v)

        ValidationRuleBuilder().fail_if("SELECT 1", name="r1").apply(mock_spark)

        self.assertEqual(conf_store[_CONF_COMMIT_PROTOCOL], _PROTOCOL_CLASS)

    def test_apply_empty_rules_sets_empty_json_array(self):
        """apply() with no rules sets an empty JSON array."""
        mock_spark = MagicMock()
        conf_store = {}
        mock_spark.conf.set.side_effect = lambda k, v: conf_store.__setitem__(k, v)

        ValidationRuleBuilder().apply(mock_spark)

        rules = json.loads(conf_store[_CONF_RULES])
        self.assertEqual(rules, [])

    def test_fail_if_chaining_preserves_order(self):
        """fail_if() calls preserve insertion order when apply() serialises them."""
        mock_spark = MagicMock()
        conf_store = {}
        mock_spark.conf.set.side_effect = lambda k, v: conf_store.__setitem__(k, v)

        builder = (
            ValidationRuleBuilder()
            .fail_if("SELECT 1", name="first")
            .fail_if("SELECT 2", name="second")
            .fail_if("SELECT 3", name="third")
        )
        builder.apply(mock_spark)

        rules = json.loads(conf_store[_CONF_RULES])
        self.assertEqual([r["name"] for r in rules], ["first", "second", "third"])

    # -----------------------------------------------------------------------
    # 8.12 from_file() loads rules and further fail_if() calls append
    # -----------------------------------------------------------------------

    def _make_mock_spark_with_hdfs_content(self, json_str: str) -> MagicMock:
        """Build a minimal mock SparkSession that serves json_str from a fake HDFS path."""
        content_bytes = json_str.encode("utf-8")

        # Mock InputStream that reads one byte at a time
        byte_iter = iter(content_bytes)
        mock_stream = MagicMock()
        mock_stream.read.side_effect = lambda: next(byte_iter, -1)
        mock_stream.close.return_value = None

        mock_fs = MagicMock()
        mock_fs.open.return_value = mock_stream

        mock_path_instance = MagicMock()
        mock_path_instance.getFileSystem.return_value = mock_fs

        mock_jvm = MagicMock()
        mock_jvm.org.apache.hadoop.fs.Path.return_value = mock_path_instance

        mock_spark = MagicMock()
        mock_spark.sparkContext._jvm = mock_jvm
        mock_spark.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock()

        return mock_spark

    def test_from_file_loads_two_rules(self):
        """from_file() returns a builder populated with the rules from the JSON file."""
        rules_json = json.dumps([
            {"name": "r1", "failIf": "SELECT 1", "description": "rule one"},
            {"name": "r2", "failIf": "SELECT 2", "description": "rule two"},
        ])
        mock_spark = self._make_mock_spark_with_hdfs_content(rules_json)
        builder = ValidationRuleBuilder.from_file(mock_spark, "hdfs:///fake/rules.json")

        self.assertEqual(len(builder._rules), 2)
        self.assertEqual(builder._rules[0]["name"], "r1")
        self.assertEqual(builder._rules[1]["name"], "r2")

    def test_from_file_appends_new_rules(self):
        """Additional fail_if() calls after from_file() append to the loaded rules."""
        rules_json = json.dumps([
            {"name": "existing", "failIf": "SELECT 1", "description": "pre-loaded"},
        ])
        mock_spark = self._make_mock_spark_with_hdfs_content(rules_json)
        builder = ValidationRuleBuilder.from_file(mock_spark, "hdfs:///fake/rules.json")
        builder.fail_if("SELECT 2", name="appended", description="new rule")

        self.assertEqual(len(builder._rules), 2)
        self.assertEqual(builder._rules[0]["name"], "existing")
        self.assertEqual(builder._rules[1]["name"], "appended")

    def test_from_file_handles_missing_description(self):
        """from_file() uses empty string when 'description' is absent from a rule object."""
        rules_json = json.dumps([
            {"name": "no_desc", "failIf": "SELECT 1"},
        ])
        mock_spark = self._make_mock_spark_with_hdfs_content(rules_json)
        builder = ValidationRuleBuilder.from_file(mock_spark, "hdfs:///fake/rules.json")

        self.assertEqual(builder._rules[0]["description"], "")


if __name__ == "__main__":
    unittest.main()
