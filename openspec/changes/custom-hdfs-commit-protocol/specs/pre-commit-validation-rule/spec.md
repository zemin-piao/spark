## ADDED Requirements

### Requirement: SqlValidationRule data type
The system SHALL provide a `SqlValidationRule` case class in `org.apache.spark.internal.io` with three `String` fields: `name` (a unique identifier for the rule), `failIf` (a SQL query that, when it returns any rows, signals a validation failure), and `description` (a human-readable label for error reporting). `SqlValidationRule` SHALL be serializable so that instances can be transferred from the driver to executors if needed.

#### Scenario: Instantiate a rule with all required fields
- **WHEN** a caller constructs `SqlValidationRule("no_nulls", "SELECT * FROM __output__ WHERE user_id IS NULL", "user_id must never be null")`
- **THEN** the resulting object exposes `name = "no_nulls"`, `failIf = "SELECT * FROM __output__ WHERE user_id IS NULL"`, and `description = "user_id must never be null"`

#### Scenario: Rule is serializable
- **WHEN** a `SqlValidationRule` instance is serialized and deserialized via Java serialization
- **THEN** all three fields retain their original values with no exception thrown

---

### Requirement: RuleFailure data type
The system SHALL provide a `RuleFailure` data type (case class or equivalent) in `org.apache.spark.internal.io` that captures the outcome of a single failed validation rule. It SHALL carry: `name` (the rule's name), `description` (the rule's description), `failIfQuery` (the executed SQL), and `sampleRows` (up to 10 rows returned by the `failIf` query, represented as a `Seq[Row]` or equivalent string representation suitable for error messages).

#### Scenario: RuleFailure holds sample rows
- **WHEN** a rule's `failIf` query returns 50 rows
- **THEN** the `RuleFailure` for that rule contains at most 10 of those rows in `sampleRows`

#### Scenario: RuleFailure holds zero sample rows for aggregate failures
- **WHEN** a rule's `failIf` aggregate query (e.g. `SELECT COUNT(*) < 1000 FROM __output__`) returns a single row
- **THEN** the `RuleFailure` `sampleRows` contains that single row (the count result)

---

### Requirement: CommitValidationException
The system SHALL provide a `CommitValidationException` class in `org.apache.spark.internal.io` that extends `RuntimeException`. It SHALL accept a `Seq[RuleFailure]` and produce a human-readable message listing every failed rule with its name, description, `failIf` query, and a formatted sample of offending rows. Passed rules SHALL also be summarised (name only, marked PASSED) in the message. The message header SHALL state the count of failed rules versus total rules (e.g. `"2 of 4 validation rules failed"`).

#### Scenario: Exception message lists all failures
- **WHEN** `CommitValidationException` is constructed with two `RuleFailure` instances (rule A and rule B)
- **THEN** calling `getMessage` returns a string that contains both rule names and their descriptions, the `failIf` queries, and sample row data

#### Scenario: Exception message header shows failure ratio
- **WHEN** `CommitValidationException` is constructed with 2 failures out of 4 total rules
- **THEN** `getMessage` starts with a line containing `"2 of 4 validation rules failed"`

#### Scenario: Zero failures — exception is not thrown
- **WHEN** all rules pass (no `RuleFailure` instances)
- **THEN** `CommitValidationException` is never constructed and no exception propagates to the caller
