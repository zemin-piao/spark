## Why

Spark's built-in `HadoopMapReduceCommitProtocol` provides no extension point for user-defined validation before a job's output is committed to HDFS. Teams that need to enforce data-quality rules (e.g., row-count checks, schema conformance, non-empty output) have no clean way to abort a commit and propagate a meaningful failure back to the Spark task without patching Spark itself.

## What Changes

- Introduce a new `ValidationRule` trait that users implement to express arbitrary pre-commit checks.
- Introduce `HdfsValidatingCommitProtocol`, a concrete `FileCommitProtocol` subclass that:
  - Delegates HDFS write/staging mechanics to `HadoopMapReduceCommitProtocol` (via inheritance).
  - Accepts an ordered sequence of `ValidationRule` instances at construction time.
  - Overrides `commitJob` to execute all registered rules against the staged output **before** calling the underlying committer; if any rule returns a failure, the job is aborted and an exception is thrown to fail the Spark task.
- Provide a `CommitValidationException` to carry structured failure information (rule name + message) to the driver.

## Capabilities

### New Capabilities

- `hdfs-validating-commit-protocol`: The concrete commit protocol class that writes to HDFS and supports injection of pre-commit validation rules.
- `pre-commit-validation-rule`: The `ValidationRule` trait and `CommitValidationException` that together define the contract for user-supplied validation logic.

### Modified Capabilities

_(none — no existing spec-level requirements change)_

## Impact

- **New files**: two new Scala source files under `core/src/main/scala/org/apache/spark/internal/io/` (one for the protocol class, one for the validation trait and exception).
- **Dependencies**: relies on existing `FileCommitProtocol`, `HadoopMapReduceCommitProtocol`, and Hadoop's `FileSystem` / `Path` APIs — no new library dependencies.
- **Configurability**: `HdfsValidatingCommitProtocol` must remain serializable (rules are serialized to executors). Rules that cannot be serialized must be documented as driver-only.
- **Failure semantics**: a validation failure causes `abortJob` to be called and re-throws as a `CommitValidationException`, which propagates as a Spark task failure with a human-readable message.
- **No breaking changes** to existing `FileCommitProtocol` or `HadoopMapReduceCommitProtocol`.
