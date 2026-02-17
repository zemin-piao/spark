## ADDED Requirements

### Requirement: Protocol class and constructor signature
The system SHALL provide a class `HdfsValidatingCommitProtocol` in `org.apache.spark.internal.io` that extends `HadoopMapReduceCommitProtocol`. It SHALL expose the same constructor signatures as its parent — a two-argument constructor `(jobId: String, path: String)` and a three-argument constructor `(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)` — so that `FileCommitProtocol.instantiate()` can reflectively construct it when `spark.sql.sources.commitProtocolClass` is set to its fully-qualified name.

#### Scenario: Reflection-based instantiation with two arguments
- **WHEN** `FileCommitProtocol.instantiate("org.apache.spark.internal.io.HdfsValidatingCommitProtocol", jobId, path)` is called
- **THEN** an instance of `HdfsValidatingCommitProtocol` is returned without error

#### Scenario: Reflection-based instantiation with three arguments
- **WHEN** `FileCommitProtocol.instantiate("org.apache.spark.internal.io.HdfsValidatingCommitProtocol", jobId, path, dynamicPartitionOverwrite = true)` is called
- **THEN** an instance of `HdfsValidatingCommitProtocol` is returned without error

---

### Requirement: Rule configuration precedence
The system SHALL resolve validation rules using the following precedence (first non-empty source wins):
1. The return value of `validationRules()`, an overridable method that returns `Seq[SqlValidationRule]` (default: empty).
2. Inline JSON in Spark conf key `spark.io.hdfs.commit.validationRules` (a JSON array of rule objects).
3. A JSON file path in Spark conf key `spark.io.hdfs.commit.validationRulesFile`, pointing to an HDFS file whose content is a JSON array of rule objects.

If none of the three sources provides rules, `commitJob` SHALL proceed with no validation (committing normally).

#### Scenario: Scala override takes precedence over conf
- **WHEN** a subclass overrides `validationRules()` to return two rules AND `spark.io.hdfs.commit.validationRules` is also set
- **THEN** only the two rules from the override are executed; the conf value is ignored

#### Scenario: Inline JSON conf used when override is empty
- **WHEN** `validationRules()` returns an empty `Seq` AND `spark.io.hdfs.commit.validationRules` is set to a valid JSON array of one rule
- **THEN** that one rule is executed at commit time

#### Scenario: File-backed conf used when override and inline conf are both empty
- **WHEN** `validationRules()` is empty, `spark.io.hdfs.commit.validationRules` is unset, and `spark.io.hdfs.commit.validationRulesFile` points to a valid HDFS JSON file with two rules
- **THEN** those two rules are executed at commit time

#### Scenario: No rules — commit proceeds normally
- **WHEN** all three sources are empty or unset
- **THEN** `commitJob` calls `super.commitJob(...)` without executing any validation queries

---

### Requirement: Staged output registration as temp view
The system SHALL read the staged output files from the staging directory (inherited `stagingDir`) before executing any validation rules. The staged files SHALL be read using the format specified by `spark.io.hdfs.commit.validationInputFormat` (default: `parquet`) with `recursiveFileLookup = true`. The resulting DataFrame SHALL be registered as the Spark SQL temp view `__output__` so that all `failIf` queries can reference it.

#### Scenario: Staged Parquet files are queryable as __output__
- **WHEN** validation rules are registered and a Parquet write job completes its tasks
- **THEN** the `failIf` query `SELECT COUNT(*) FROM __output__` returns the correct row count of the staged output

#### Scenario: Non-default input format is honoured
- **WHEN** `spark.io.hdfs.commit.validationInputFormat` is set to `orc`
- **THEN** staged ORC files are read and registered as `__output__` instead of Parquet

#### Scenario: Staging directory read fails with clear error
- **WHEN** the staged files cannot be read (e.g. format mismatch)
- **THEN** an exception is thrown with a message that references `spark.io.hdfs.commit.validationInputFormat` to guide the user

---

### Requirement: Collect-all validation with single-scan fusion
The system SHALL execute all registered validation rules regardless of whether any individual rule fails, collecting every failure before deciding to abort. Rules SHALL be fused into a single Spark action (one HDFS read pass over the staged output) by grouping aggregate rules into a single multi-aggregate `SELECT` and row-returning rules into a `UNION ALL` query. If a rule cannot be fused (e.g. mixed aggregate/row-returning patterns in one query), it SHALL fall back to individual execution and a warning SHALL be logged.

#### Scenario: All rules evaluated even after first failure
- **WHEN** rule A fails and rule B is registered after rule A
- **THEN** rule B is still executed and its result (pass or fail) is included in the final report

#### Scenario: Single read pass for multiple rules
- **WHEN** three row-returning rules are registered
- **THEN** the staged data is read exactly once (verified by a single Spark job submission for all three rules)

#### Scenario: Unfusable rule falls back gracefully
- **WHEN** a rule cannot be classified as aggregate or row-returning
- **THEN** it executes as a standalone query, a WARN-level log message is emitted, and the result is still included in the final validation report

---

### Requirement: Auto-LIMIT on row-returning failIf queries
The system SHALL automatically append `LIMIT 10` to any `failIf` query that does not already contain a `LIMIT` clause (case-insensitive). This applies at rule-execution time, not at rule-creation time, so the original `failIf` string stored in `SqlValidationRule` is unchanged.

#### Scenario: LIMIT appended to query without one
- **WHEN** a rule's `failIf` is `"SELECT * FROM __output__ WHERE user_id IS NULL"` (no LIMIT)
- **THEN** the executed query is `"SELECT * FROM __output__ WHERE user_id IS NULL LIMIT 10"`

#### Scenario: Existing LIMIT clause is not duplicated
- **WHEN** a rule's `failIf` already contains `LIMIT 5`
- **THEN** the query is executed as-is, without an additional LIMIT clause

#### Scenario: Aggregate rules are not given a LIMIT
- **WHEN** a rule's `failIf` is `"SELECT COUNT(*) < 1000 FROM __output__"` (an aggregate with no row scan)
- **THEN** no LIMIT clause is appended

---

### Requirement: Validation failure path — collect then abort
The system SHALL, upon detecting one or more rule failures in `commitJob`: (1) call `super.abortJob(jobContext)` to clean up staged files, then (2) throw a `CommitValidationException` containing all `RuleFailure` instances. If no rules fail, the system SHALL call `super.commitJob(jobContext, taskCommits)` normally. The `CommitValidationException` SHALL propagate as a Spark task failure with its human-readable message visible on the driver.

#### Scenario: Validation failure triggers abortJob and exception
- **WHEN** one or more rules fail
- **THEN** `abortJob` is called to clean up staging, and a `CommitValidationException` is thrown before `super.commitJob` is reached

#### Scenario: All rules pass — commitJob proceeds
- **WHEN** all registered rules return zero rows from their `failIf` queries
- **THEN** `super.commitJob(jobContext, taskCommits)` is called and no exception is thrown

#### Scenario: SparkSession unavailable — clear error
- **WHEN** `SparkSession.getActiveSession` returns `None` at `commitJob` time (e.g. in a bare unit-test context)
- **THEN** an `IllegalStateException` is thrown with a message explaining that an active SparkSession is required for validation

---

### Requirement: Python ValidationRuleBuilder fluent API
The system SHALL provide a Python class `ValidationRuleBuilder` in `pyspark.internal.io` (or an equivalent PySpark-accessible location) with:
- `fail_if(query: str, *, name: str, description: str = "") -> ValidationRuleBuilder` — adds a rule and returns `self` for chaining.
- `apply(spark: SparkSession) -> None` — serialises rules as a JSON array into `spark.io.hdfs.commit.validationRules` and sets `spark.sql.sources.commitProtocolClass` to `HdfsValidatingCommitProtocol`.
- `save_and_apply(spark: SparkSession, path: str) -> None` — writes rules as a JSON file to `path` on HDFS, sets `spark.io.hdfs.commit.validationRulesFile` to `path`, and sets `spark.sql.sources.commitProtocolClass`.
- `from_file(spark: SparkSession, path: str) -> ValidationRuleBuilder` — class method that loads rules from an HDFS JSON file and returns a populated builder.

#### Scenario: apply() sets conf keys
- **WHEN** a builder with two rules has `apply(spark)` called
- **THEN** `spark.conf.get("spark.io.hdfs.commit.validationRules")` returns a valid JSON array of two rule objects AND `spark.conf.get("spark.sql.sources.commitProtocolClass")` equals the FQCN of `HdfsValidatingCommitProtocol`

#### Scenario: save_and_apply() writes file and sets file-path conf
- **WHEN** `save_and_apply(spark, "hdfs:///rules/my-rules.json")` is called
- **THEN** a JSON file exists at `hdfs:///rules/my-rules.json` containing the rules array AND `spark.conf.get("spark.io.hdfs.commit.validationRulesFile")` equals `"hdfs:///rules/my-rules.json"`

#### Scenario: from_file() loads existing rules
- **WHEN** `ValidationRuleBuilder.from_file(spark, "hdfs:///rules/shared.json")` is called and the file contains two rules
- **THEN** the returned builder contains those two rules and further `fail_if()` calls append to them

#### Scenario: Chained fail_if() calls preserve order
- **WHEN** three `fail_if()` calls are chained on a single builder and `apply(spark)` is called
- **THEN** the rules are stored and applied in the order they were added
