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
Fluent API for configuring SQL-based pre-commit validation rules with
:class:`~pyspark.sql.execution.datasources.HdfsValidatingCommitProtocol`.
"""

from __future__ import annotations

import json
from typing import List, Dict, Any

__all__ = ["ValidationRuleBuilder"]

_PROTOCOL_CLASS = (
    "org.apache.spark.sql.execution.datasources.HdfsValidatingCommitProtocol"
)
_CONF_RULES = "spark.io.hdfs.commit.validationRules"
_CONF_RULES_FILE = "spark.io.hdfs.commit.validationRulesFile"
_CONF_COMMIT_PROTOCOL = "spark.sql.sources.commitProtocolClass"


class ValidationRuleBuilder:
    """Fluent builder for SQL pre-commit validation rules.

    Rules are expressed as SQL ``SELECT`` assertions using a ``fail_if``
    convention: if the query returns any rows, the rule fails and the write
    job is aborted before output is finalised.

    Example::

        from pyspark.sql.hdfs_validation import ValidationRuleBuilder

        (
            ValidationRuleBuilder()
            .fail_if(
                "SELECT * FROM __output__ WHERE user_id IS NULL",
                name="no_null_user_ids",
                description="user_id must never be null",
            )
            .fail_if(
                "SELECT COUNT(*) < 1000 FROM __output__",
                name="min_row_count",
                description="Output must have at least 1000 rows",
            )
            .apply(spark)
        )

        df.write.mode("overwrite").parquet("hdfs:///output/my-dataset")
    """

    def __init__(self) -> None:
        self._rules: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Rule registration
    # ------------------------------------------------------------------

    def fail_if(
        self,
        query: str,
        *,
        name: str,
        description: str = "",
    ) -> "ValidationRuleBuilder":
        """Add a validation rule.

        Parameters
        ----------
        query:
            SQL query.  If it returns any rows the rule fails and the write
            job is aborted.
        name:
            Unique identifier used in error reports.
        description:
            Human-readable label included in the exception message.

        Returns
        -------
        self
            The builder, for method chaining.
        """
        self._rules.append(
            {"name": name, "failIf": query, "description": description}
        )
        return self

    # ------------------------------------------------------------------
    # Conf application methods
    # ------------------------------------------------------------------

    def apply(self, spark: Any) -> None:
        """Serialise rules as inline JSON and configure the Spark session.

        Sets:

        * ``spark.io.hdfs.commit.validationRules`` — JSON array of rules.
        * ``spark.sql.sources.commitProtocolClass`` —
          ``HdfsValidatingCommitProtocol``.

        Parameters
        ----------
        spark:
            Active :class:`~pyspark.sql.SparkSession`.
        """
        json_str = json.dumps(self._rules)
        spark.conf.set(_CONF_RULES, json_str)
        spark.conf.set(_CONF_COMMIT_PROTOCOL, _PROTOCOL_CLASS)

    def save_and_apply(self, spark: Any, path: str) -> None:
        """Write rules to an HDFS JSON file and configure the Spark session.

        Useful when the inline JSON would be too large for Spark conf (e.g.
        complex SQL with CTEs, or many rules).

        Sets:

        * ``spark.io.hdfs.commit.validationRulesFile`` — the HDFS path.
        * ``spark.sql.sources.commitProtocolClass`` —
          ``HdfsValidatingCommitProtocol``.

        Parameters
        ----------
        spark:
            Active :class:`~pyspark.sql.SparkSession`.
        path:
            HDFS (or compatible filesystem) path where the JSON file will be
            written, e.g. ``"hdfs:///shared/rules/my-job-rules.json"``.
        """
        json_bytes = json.dumps(self._rules).encode("utf-8")

        # Write via Hadoop FileSystem API through the JVM gateway
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = fs_path.getFileSystem(hadoop_conf)
        out = fs.create(fs_path, True)  # overwrite=True
        try:
            out.write(json_bytes)
        finally:
            out.close()

        spark.conf.set(_CONF_RULES_FILE, path)
        spark.conf.set(_CONF_COMMIT_PROTOCOL, _PROTOCOL_CLASS)

    # ------------------------------------------------------------------
    # Factory method
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, spark: Any, path: str) -> "ValidationRuleBuilder":
        """Load rules from an existing HDFS JSON file.

        The returned builder is pre-populated with the rules from the file.
        Additional :meth:`fail_if` calls append to them.

        Parameters
        ----------
        spark:
            Active :class:`~pyspark.sql.SparkSession`.
        path:
            HDFS path to a JSON file containing an array of rule objects
            (``name``, ``failIf``, ``description``).

        Returns
        -------
        ValidationRuleBuilder
            A new builder pre-populated with the loaded rules.
        """
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = fs_path.getFileSystem(hadoop_conf)
        in_stream = fs.open(fs_path)
        try:
            # Read all bytes via Java InputStream
            buf = bytearray()
            b = in_stream.read()
            while b != -1:
                buf.append(b)
                b = in_stream.read()
            json_str = buf.decode("utf-8")
        finally:
            in_stream.close()

        rules = json.loads(json_str)
        builder = cls()
        for rule in rules:
            builder.fail_if(
                rule["failIf"],
                name=rule["name"],
                description=rule.get("description", ""),
            )
        return builder
