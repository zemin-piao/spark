## Context

Spark's `FileCommitProtocol` lifecycle runs on the driver (`setupJob`, `commitJob`, `abortJob`) and on executors (`setupTask`, `commitTask`, `abortTask`). The existing `HadoopMapReduceCommitProtocol` handles HDFS staging and commit via Hadoop's `FileOutputCommitter`, but exposes no hook for user-defined validation before output is finalised.

Two user groups need this capability:
- **Scala/Java engineers**: want to subclass and override programmatically.
- **Data engineers (PySpark users)**: want to write SQL assertions without touching Scala, using a Python-native fluent API.

The protocol class is always instantiated via `FileCommitProtocol.instantiate()` using reflection with a fixed constructor signature `(jobId: String, path: String[, dynamicPartitionOverwrite: Boolean])`, which rules out constructor injection of rule objects when registered via `spark.sql.sources.commitProtocolClass`.

**Scope**: `spark.sql.sources.commitProtocolClass` is read by exactly three entry points in Spark core — `InsertIntoHadoopFsRelationCommand`, `FileWrite.toBatch()`, and `SaveAsHiveFile`. Delta Lake, Apache Iceberg, and Apache Hudi all manage their own commit paths independently and are **unaffected** by this config.

## Goals / Non-Goals

**Goals:**
- Provide a concrete `FileCommitProtocol` subclass that writes to HDFS via the existing Hadoop staging mechanism.
- Allow validation rules to be expressed as SQL `SELECT` assertions executed against the staged output before commit.
- Run **all** validation rules regardless of individual failures; report all failures together in one exception.
- Support two authoring surfaces: a Python `ValidationRuleBuilder` fluent API (for data engineers) and a Scala `validationRules()` override (for Scala engineers).
- On validation failure: abort the job cleanly and surface a structured, human-readable error listing every failed rule with sample offending rows.

**Non-Goals:**
- YAML/declarative rule files (lower priority, not in this change).
- Executor-side (per-task) validation — validation runs on the driver only, after all tasks complete.
- Supporting non-HDFS output paths (S3, GCS) in this change.
- Changing the `FileCommitProtocol` base class or `HadoopMapReduceCommitProtocol`.
- Validation for Delta Lake, Apache Iceberg, or Apache Hudi writes — those formats bypass `spark.sql.sources.commitProtocolClass` entirely and require their own native mechanisms.
- Structured Streaming — streaming file sinks use a separate config key (`spark.sql.streaming.commitProtocolClass`) and are out of scope for this change.

## Decisions

### 1. Inheritance over composition

`HdfsValidatingCommitProtocol` extends `HadoopMapReduceCommitProtocol` rather than wrapping it.

**Why**: The parent class has `protected lazy val stagingDir` and `protected def setupCommitter(...)` which are needed to locate staged files and set up the Hadoop committer. Composition would require duplicating or re-implementing that internals access. Inheritance is the pattern already used in the Spark codebase (`SQLHadoopMapReduceCommitProtocol` also extends `HadoopMapReduceCommitProtocol`).

**Alternative considered**: Wrap an instance via delegation. Rejected because `FileCommitProtocol` is not an interface — delegation would require delegating all abstract methods and would not benefit from `protected` members.

---

### 2. SQL-first validation model

Rules are expressed as SQL `SELECT` assertions using a `failIf` query convention: **if the query returns any rows, the rule fails**.

```
rule.failIf = "SELECT * FROM __output__ WHERE user_id IS NULL"
              ↑ returns rows → validation fails, commit aborted
```

The staged output files are read into a temporary Spark DataFrame and registered as the temp view `__output__` before any rule query runs. Rules execute as standard Spark SQL queries (distributed reads, driver-side result check).

**Why `failIf` semantics over `passIf`**: A failing assertion that returns the *offending rows* is far more debuggable — the exception message can include a sample of the rows that caused the failure, which is what data engineers need.

**Alternative considered**: `passIf` convention (query must return a single `true` row). Rejected because it produces less informative errors and is less natural for SQL writers.

---

### 3. Collect-all failure reporting — no fail-fast

All validation rules are executed regardless of whether earlier rules fail. Failures are collected into a list, and a single `CommitValidationException` is thrown at the end containing all failures.

**Why**: Fail-fast forces users to fix one rule, re-run a potentially long job, discover the next failure, and repeat. Collect-all surfaces every issue in one run — the same convention used by dbt tests and pytest by default.

**Failure report format**:
```
CommitValidationException: 2 of 4 validation rules failed

  FAILED  min_row_count — "Output must have at least 1000 rows"
          Query: SELECT COUNT(*) < 1000 FROM __output__
          Result: 1 row returned (count was 342)

  FAILED  no_null_user_ids — "user_id must never be null"
          Query: SELECT * FROM __output__ WHERE user_id IS NULL
          Sample (5 of 1,204 offending rows):
          ┌──────────┬─────────┬─────────────────────┐
          │ order_id │ user_id │ created_at          │
          ├──────────┼─────────┼─────────────────────┤
          │ 8821     │ null    │ 2024-01-15 08:23:11 │
          └──────────┴─────────┴─────────────────────┘

  PASSED  no_negative_revenue
  PASSED  schema_conformance
```

Each failed rule shows: name, description, the `failIf` query, and up to 10 sample offending rows.

---

### 4. Python `ValidationRuleBuilder` — primary interface for data engineers

Data engineers interact with validation rules exclusively through a Python fluent API. They never write JSON or set conf keys manually.

```python
from pyspark.internal.io import ValidationRuleBuilder

rules = (
    ValidationRuleBuilder()
    .fail_if(
        "SELECT COUNT(*) < 1000 FROM __output__",
        name="min_row_count",
        description="Output must have at least 1000 rows"
    )
    .fail_if(
        "SELECT * FROM __output__ WHERE user_id IS NULL",
        name="no_null_user_ids",
        description="user_id must never be null"
    )
)

# Option A: inline — serialises rules as JSON into Spark conf (for simple/few rules)
rules.apply(spark)

# Option B: file-backed — writes rules.json to HDFS, sets file path in conf (for complex/many rules)
rules.save_and_apply(spark, "hdfs:///shared/rules/my-job-rules.json")

# Option C: load a shared rules file (reuse across jobs / teams)
rules = ValidationRuleBuilder.from_file(spark, "hdfs:///shared/rules/team-standard-rules.json")
rules.apply(spark)
```

`apply()` and `save_and_apply()` both set `spark.sql.sources.commitProtocolClass` automatically. The write call itself is unchanged:

```python
df.write.mode("overwrite").parquet("hdfs:///output/my-dataset")
```

**Why a builder rather than raw conf**: Removes JSON encoding boilerplate, prevents malformed JSON errors, provides IDE autocompletion, and is discoverable without reading docs.

---

### 5. Two-tier rule configuration — inline JSON and file-backed

Inline JSON in Spark conf (`spark.io.hdfs.commit.validationRules`) is convenient for simple cases but has practical size constraints:

- **Kubernetes**: Spark configs are stored in a `ConfigMap` with a ~1.5 MB total budget (etcd limit, enforced in `KubernetesClientUtils`). Large JSON values compete with all other configs.
- **spark-submit `--conf`**: Subject to OS `ARG_MAX` (~2 MB on Linux, shared across all args).
- **YARN**: Configs are embedded in `ApplicationSubmissionContext`; very large values degrade RPC performance.

For complex SQL (CTEs, long subqueries) or many rules, the file-backed approach stores rules in a JSON file on HDFS and puts only the path in conf (`spark.io.hdfs.commit.validationRulesFile`). `ValidationRuleBuilder.save_and_apply()` handles this automatically.

**Precedence**: Scala `validationRules()` override → inline JSON conf → file conf. First non-empty source wins.

---

### 6. Scala override surface — secondary interface for Scala engineers

Users subclass `HdfsValidatingCommitProtocol` and override `validationRules()` for programmatic control:

```scala
class MyJobCommitProtocol(jobId: String, path: String)
    extends HdfsValidatingCommitProtocol(jobId, path) {
  override def validationRules: Seq[SqlValidationRule] = Seq(
    SqlValidationRule("min_rows", "SELECT COUNT(*) < 1000 FROM __output__", "At least 1000 rows required"),
    SqlValidationRule("no_null_ids", "SELECT * FROM __output__ WHERE user_id IS NULL", "No null user_ids allowed")
  )
}
```

---

### 7. Validation timing — pre-commit, against the staging directory

Validation runs inside `commitJob` **before** calling `super.commitJob(...)`. At this point all tasks have completed their `commitTask`, so staged files are fully written but not yet moved to the final output path.

The staged output path is read using `stagingDir` (inherited `protected lazy val` from the parent class) with `recursiveFileLookup = true` to handle both FileOutputCommitter algorithm v1 and v2 path structures. The input format is configurable via `spark.io.hdfs.commit.validationInputFormat` (default: `parquet`).

**Why pre-commit**: Post-commit validation would require deleting already-committed files on failure — a non-atomic, error-prone rollback. Pre-commit validation aborts cleanly using the existing `abortJob` path.

---

### 8. SparkSession access at commit time

SQL validation requires a `SparkSession` to execute queries. `SparkSession.getActiveSession` is used, which is always populated on the driver during a Spark write operation. If no active session is available (e.g., unit-test context), an `IllegalStateException` is thrown with a clear message rather than silently skipping validation.

---

### 9. Failure path — collect all, then abort

On `commitJob`:
1. Run every rule's `failIf` query against `__output__`.
2. For each rule that returns rows: collect a `RuleFailure(name, description, sampleRows)` (up to 10 rows).
3. After all rules have run: if any failures exist, call `super.abortJob(jobContext)` to clean up staging, then throw `CommitValidationException(failures)` with the formatted report.
4. If no failures: proceed with `super.commitJob(jobContext, taskCommits)` as normal.

---

### 10. Single-scan rule fusion — one read pass regardless of rule count

By default, executing N rules as N independent Spark jobs means N full HDFS read passes over the staged output. For a 1 TB write with 5 rules, that is 5 TB of extra reads before commit.

The protocol internally **fuses all rules into a single Spark job** using `UNION ALL`, so staged data is read exactly once:

```sql
-- Internally generated; user only sees individual .fail_if(...) calls
SELECT 'min_rows'      AS __rule__, COUNT(*) AS __count__, NULL AS __sample__
  FROM __output__ HAVING COUNT(*) < 1000
UNION ALL
SELECT 'no_null_ids'   AS __rule__, COUNT(*) AS __count__, CAST(user_id AS STRING)
  FROM __output__ WHERE user_id IS NULL LIMIT 10
```

The engine splits rules into two groups by shape:
- **Aggregate rules** (e.g. `COUNT(*)`, `MAX(col)`) — fused into one multi-aggregate `SELECT` using `HAVING` to filter failures.
- **Row-returning rules** (e.g. `SELECT * FROM __output__ WHERE ...`) — fused via `UNION ALL`.

Both groups execute as a single Spark action. This means **1 read pass regardless of how many rules are registered**.

**Parquet column statistics short-circuit**: for Parquet output (the default), the Spark SQL engine pushes predicates into file-level metadata reads. Null checks, range checks, and `COUNT(*)` are often answered from footer statistics without reading actual row data — making common rules nearly free.

---

### 11. Auto-LIMIT on row-returning rules

Row-returning `failIf` queries that do not include a `LIMIT` clause will scan the entire staged dataset just to collect offending rows. The protocol only needs a sample (up to 10 rows) to produce a useful error.

`ValidationRuleBuilder.fail_if()` automatically appends `LIMIT 10` to any query that does not already contain a `LIMIT` clause:

```python
# User writes:
.fail_if("SELECT * FROM __output__ WHERE user_id IS NULL")

# Protocol executes:
# SELECT * FROM __output__ WHERE user_id IS NULL LIMIT 10
```

This caps the scan at the first 10 offending rows, which combined with Parquet predicate pushdown means most row-returning rules terminate very quickly once a violation is found.

Users who need to count all violations (e.g. to report a percentage) should write aggregate rules instead:

```python
# Cheap — answered from Parquet stats or a single aggregation pass
.fail_if("SELECT COUNT(*) < 1000 FROM __output__", name="min_rows")

# Also cheap — stops at first violation
.fail_if("SELECT * FROM __output__ WHERE user_id IS NULL", name="no_nulls")
```

## Risks / Trade-offs

- **Staging directory format assumption** → The protocol reads staged files using a configured format (default Parquet). If the write job uses a different format and the user does not set `spark.io.hdfs.commit.validationInputFormat`, the read will fail with a confusing error. Mitigation: log a clear error if the read fails, pointing to the config key.

- **Validation adds an extra read phase** → Even with single-scan fusion, validation adds at least one full read pass over the staged output. For very large datasets (100 GB+) this is a meaningful cost. Mitigation: document clearly that validation is a trade-off; Parquet column statistics and auto-LIMIT minimise the cost for common rule patterns. Users may optionally disable validation per-job by registering no rules.

- **Rule fusion assumes compatible SQL shapes** → Aggregate and row-returning rules are fused separately; rules that mix both patterns in one query (e.g. a subquery returning a scalar used in a `WHERE`) fall back to individual execution. Mitigation: document supported rule shapes; log a warning when a rule cannot be fused.

- **`SparkSession.getActiveSession` may return `None` in tests** → Validation throws immediately rather than silently passing. Mitigation: test harnesses must provide a real `SparkSession`, or override `validationRules()` to return empty.

- **Staged path differs between FileOutputCommitter v1 and v2** → Mitigated by reading from `stagingDir` recursively (`recursiveFileLookup = true`).

- **`SqlValidationRule` must be serializable** → `SqlValidationRule` as a case class holding only `String` fields satisfies this automatically. Custom Scala subclasses holding non-serialisable state must mark those fields `@transient`.

- **Delta Lake / Iceberg / Hudi writes are invisible to this protocol** → By design. Users who also write to these formats and want pre-commit validation must use those formats' own native mechanisms (Delta constraints, Iceberg write checks, Hudi write validators). This boundary must be clearly documented.

- **Streaming file sinks not covered** → Structured Streaming uses `spark.sql.streaming.commitProtocolClass` (default: `ManifestFileCommitProtocol`). Out of scope; requires a separate change if needed.

## Open Questions

- Should `spark.io.hdfs.commit.validationInputFormat` support format auto-detection (e.g., by inspecting file extensions in the staging dir)? Deferred — start with explicit conf, auto-detect can be added later.
- Should validation failures produce a partial metrics report (rules passed/failed counts) via `SparkListener`? Out of scope for now but a natural follow-on.
