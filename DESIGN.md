# PySpark support: design

## What "a PySpark version" needs

The connector is a JVM Structured Streaming source. `S3ConnectorSourceProvider` registers
the short name `s3-connector` through `META-INF/services/...DataSourceRegister`, so
PySpark can already use it with no Python code at all:

```python
spark.readStream.format("s3-connector").schema(schema).options(**opts).load()
```

What a Python user is missing today:

1. **Option names.** They live only as Scala constants in `S3ConnectorSourceOptions`
   (`spark.s3conn.*`, plus `basePath`). Python users must copy strings by hand. The SQS
   keys were also misspelled with a doubled dot (`spark.s3conn..sqs.*`) up to 0.0.2.
2. **Early validation.** Scala parses the options only when the query starts
   (`writeStream.start()` creates the source on the stream thread), so a typo surfaces
   late as a `StreamingQueryException`.
3. **The right jar.** Spark 3.x needs a Scala 2.12 jar, Spark 4.x a Scala 2.13 jar. When
   the jar is missing, `load()` fails with a generic "Failed to find data source".

## Decision: one pure-Python wheel for every Spark line

A small package, `spark-streaming-sql-s3-connector` (import `spark_s3_connector`), in
`python/`:

- `s3_connector_options(...)`: keyword-only, typed options builder. It checks the same
  rules as the Scala parser and returns the `dict[str, str]` the source reads.
- `read_stream(spark, schema, **options)`: builds the options, calls
  `spark.readStream.format("s3-connector")...load()`, and turns "data source not found"
  into `ConnectorNotFoundError` with a hint about `--jars` and the Scala version.
- `scala_binary_version(spark_version)`: `"2.12"` for 3.x, `"2.13"` for 4.x, to pick the jar.
- `examples/read_stream.py`.

**One wheel, not one per Spark line**, because the Python side touches only
`DataStreamReader.format/schema/options/load`, which are unchanged from Spark 3.2 to 4.1.
(With Spark Connect the builder and `read_stream` work, but `load()` is lazy there, so a
missing jar shows up at `start()` and is not turned into `ConnectorNotFoundError`. Connect
was not tested.) The only per-version artifact is the JVM jar, which the main Maven build
produces per Spark version (see MATRIX.md). A compatibility table in the package README maps each
EMR/Glue release to its Spark line, Scala version and jar.

Rejected:

- **One wheel per Spark line**: the Python code would be identical in each, so it only adds
  release work and lets users install the wrong one.
- **Bundling the jar in the wheel**: needs one wheel per Scala line, makes the wheel large,
  and EMR/Glue jobs already take jars via `--jars` / `--extra-jars`.
- **Resolving the jar at runtime (download, `--packages`)**: the jar is not on Maven
  Central, and fetching code at runtime is a supply-chain risk. The helper only gives a hint.

Constraints:

- `requires-python >= 3.7` (EMR 6.x ships Python 3.7). No runtime dependencies: PySpark is
  provided by the platform, so it is not a dependency of the wheel.
- SQS keys: the helper emits the one-dot keys `spark.s3conn.sqs.*`, which the fixed Scala
  code reads first (it keeps the two-dot spelling as a fallback). Jars built before that fix
  (0.0.1, 0.0.2) read only the two-dot keys and silently ignore the one-dot ones.

## Tests

- Unit tests (no Spark): builder output and validation, error wrapping.
- Per Spark version, local-mode smoke tests with the real jar: the short name resolves, the
  Python key names equal the Scala constants read over py4j, the real Scala parser accepts
  the helper's output and gives the same values, and bad options are rejected both by the
  Scala parser and by a started query. Local mode cannot reach SQS, so no message flow.
