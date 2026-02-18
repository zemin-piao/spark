## 1. Scala Data Types

- [ ] 1.1 Create `SqlValidationRule` case class in `core/src/main/scala/org/apache/spark/internal/io/` with fields `name: String`, `failIf: String`, `description: String` and `extends Serializable`
- [ ] 1.2 Create `RuleFailure` case class in the same package with fields `name: String`, `description: String`, `failIfQuery: String`, `sampleRows: Seq[Row]`
- [ ] 1.3 Create `CommitValidationException(failures: Seq[RuleFailure], allRuleNames: Seq[String])` extending `RuntimeException` with a formatted message: ratio header (`"N of M validation rules failed"`), FAILED blocks (name, description, query, sample rows), and PASSED lines (name only)

## 2. JSON Rule Serialization

- [ ] 2.1 Add a `SqlValidationRuleJson` codec (using Jackson or Scala's built-in JSON) to encode/decode a `SqlValidationRule` to/from a JSON object with keys `name`, `failIf`, `description`
- [ ] 2.2 Add `parseRulesJson(json: String): Seq[SqlValidationRule]` utility that parses a JSON array string into a `Seq[SqlValidationRule]`, throwing a clear `IllegalArgumentException` on malformed input
- [ ] 2.3 Add `readRulesFile(path: String, hadoopConf: Configuration): Seq[SqlValidationRule]` that reads a JSON file from HDFS and delegates to `parseRulesJson`

## 3. Protocol Class Skeleton

- [ ] 3.1 Create `HdfsValidatingCommitProtocol` in `core/src/main/scala/org/apache/spark/internal/io/` extending `HadoopMapReduceCommitProtocol` with both `(jobId: String, path: String)` and `(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)` constructors
- [ ] 3.2 Add `def validationRules: Seq[SqlValidationRule] = Seq.empty` as an overridable method for Scala subclass authors
- [ ] 3.3 Add `private def resolveRules(spark: SparkSession): Seq[SqlValidationRule]` implementing the three-tier precedence: (1) `validationRules()` override, (2) inline JSON from `spark.io.hdfs.commit.validationRules`, (3) file path from `spark.io.hdfs.commit.validationRulesFile`

## 4. Staged Output Registration

- [ ] 4.1 Add `private def registerStagedOutput(spark: SparkSession): Unit` that reads `stagingDir` using the format from `spark.io.hdfs.commit.validationInputFormat` (default: `parquet`) with `recursiveFileLookup = true` and registers the resulting DataFrame as temp view `__output__`
- [ ] 4.2 Wrap the staged read in a try-catch that rethrows with a message referencing `spark.io.hdfs.commit.validationInputFormat` when the read fails due to format mismatch

## 5. Rule Execution Engine

- [ ] 5.1 Add `private def isAggregate(sql: String): Boolean` that classifies a `failIf` query as aggregate (contains aggregate functions without a `WHERE` clause at the top level) vs row-returning
- [ ] 5.2 Add `private def applyAutoLimit(sql: String): String` that appends `LIMIT 10` to row-returning queries that do not already contain a case-insensitive `LIMIT` token; returns aggregate queries unchanged
- [ ] 5.3 Add aggregate fusion: combine all aggregate `failIf` queries into a single multi-aggregate `SELECT … FROM __output__` with per-rule `HAVING` conditions tagged with `__rule__` aliases
- [ ] 5.4 Add row-returning fusion: combine all row-returning `failIf` queries (after auto-LIMIT) into a single `UNION ALL` query where each branch adds a literal `__rule__` column; execute as one Spark action and split results by rule name
- [ ] 5.5 Add fallback execution for unfusable rules: run each as a standalone `spark.sql(...)` call and emit a `logWarning(...)` naming the rule that could not be fused
- [ ] 5.6 Add `private def executeRules(spark: SparkSession, rules: Seq[SqlValidationRule]): Seq[RuleFailure]` that routes rules through aggregate fusion, row-returning fusion, and fallback; returns all `RuleFailure` instances collected across all three paths

## 6. commitJob Integration

- [ ] 6.1 Override `commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage])` in `HdfsValidatingCommitProtocol`; obtain `SparkSession.getActiveSession` and throw `IllegalStateException` if `None`
- [ ] 6.2 Call `resolveRules(spark)`; if empty, delegate immediately to `super.commitJob(...)` and return
- [ ] 6.3 Call `registerStagedOutput(spark)` then `executeRules(spark, rules)` to collect failures
- [ ] 6.4 If failures non-empty: call `super.abortJob(jobContext)` then throw `CommitValidationException(failures, rules.map(_.name))`
- [ ] 6.5 If no failures: call `super.commitJob(jobContext, taskCommits)` normally

## 7. Python ValidationRuleBuilder

- [ ] 7.1 Create `python/pyspark/internal/io.py` (or append to existing module) with `ValidationRuleBuilder` class holding an internal `list` of rule dicts
- [ ] 7.2 Implement `fail_if(self, query: str, *, name: str, description: str = "") -> "ValidationRuleBuilder"` that appends a rule dict and returns `self`
- [ ] 7.3 Implement `apply(self, spark) -> None` that serialises `self._rules` to a JSON array string, sets `spark.conf.set("spark.io.hdfs.commit.validationRules", json_str)` and `spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.HdfsValidatingCommitProtocol")`
- [ ] 7.4 Implement `save_and_apply(self, spark, path: str) -> None` that writes the JSON array to `path` on HDFS via `spark.sparkContext._jvm` Hadoop FileSystem APIs, sets `spark.io.hdfs.commit.validationRulesFile` to `path`, and sets `spark.sql.sources.commitProtocolClass`
- [ ] 7.5 Implement `from_file(cls, spark, path: str) -> "ValidationRuleBuilder"` as a `@classmethod` that reads the JSON file from HDFS, parses it, and returns a builder pre-populated with those rules

## 8. Tests

- [ ] 8.1 Unit test: `SqlValidationRule` field access and Java serialization round-trip (`ObjectOutputStream` / `ObjectInputStream`)
- [ ] 8.2 Unit test: `RuleFailure.sampleRows` is capped at 10 when the query returns more rows
- [ ] 8.3 Unit test: `CommitValidationException` message contains ratio header, FAILED blocks with sample rows, and PASSED lines for non-failing rules
- [ ] 8.4 Unit test: `parseRulesJson` round-trips a `Seq[SqlValidationRule]` correctly; throws on invalid JSON
- [ ] 8.5 Unit test: `resolveRules` — override takes precedence over inline JSON, inline JSON takes precedence over file conf, empty all three → empty result
- [ ] 8.6 Unit test: `isAggregate` correctly classifies representative queries from both categories
- [ ] 8.7 Unit test: `applyAutoLimit` appends only when no `LIMIT` present; leaves aggregates and queries with existing `LIMIT` unchanged
- [ ] 8.8 Integration test (SparkFunSuite): write a small Parquet dataset with `HdfsValidatingCommitProtocol`; rules all pass → verify `commitJob` completes and output files exist at target path
- [ ] 8.9 Integration test: write Parquet with one passing and two failing rules → verify `CommitValidationException` is thrown, contains both failures, and target path is empty (aborted)
- [ ] 8.10 Integration test: register three row-returning rules; use `SparkListener` to assert exactly one Spark job is submitted during `commitJob` (single-scan fusion)
- [ ] 8.11 Unit test: `ValidationRuleBuilder.apply()` sets `spark.io.hdfs.commit.validationRules` to valid JSON and sets `commitProtocolClass`
- [ ] 8.12 Unit test: `ValidationRuleBuilder.from_file()` loads rules from a JSON string (mock HDFS read) and further `fail_if()` calls append correctly
