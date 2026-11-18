# Result: connector 0.0.3 on every Spark version used by EMR and Glue (3.2.1 up to 4.1.3)

Repo: `/workplace/gromav/spark-s3-connector-update/versions` (local clone, nothing pushed, no PR, no release, upstream untouched, no AWS changes).
All 17 versions build and pass all unit tests. No version failed.

## Matrix and scheme

See `/workplace/gromav/spark-s3-connector-update/versions/MATRIX.md` (sources per row, EMR/Glue mapping, scheme). Short form: one pom, one Maven profile per Spark line (`spark-3.2` ... `spark-4.1`), exact release via `-Dspark.version`, tag `v0.0.3-spark<version>`.
Command (JDK 11 for 3.x, 17 for 4.x): `mvn -Pdefault-test,<profile> -Dspark.version=<version> test`.

## Per-version test results (all on commit 82c71c3, the commit every tag points at)

| Spark | Profile / JDK | Unit tests | Integration | Log | Tag |
|---|---|---|---|---|---|
| 3.2.1 | spark-3.2 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.2.1.log` | `v0.0.3-spark3.2.1` |
| 3.3.0 | spark-3.3 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.3.0.log` | `v0.0.3-spark3.3.0` |
| 3.3.1 | spark-3.3 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.3.1.log` | `v0.0.3-spark3.3.1` |
| 3.3.2 | spark-3.3 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.3.2.log` | `v0.0.3-spark3.3.2` |
| 3.4.0 | spark-3.4 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.4.0.log` | `v0.0.3-spark3.4.0` |
| 3.4.1 | spark-3.4 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.4.1.log` | `v0.0.3-spark3.4.1` |
| 3.5.0 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.0.log` | `v0.0.3-spark3.5.0` |
| 3.5.1 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.1.log` | `v0.0.3-spark3.5.1` |
| 3.5.2 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.2.log` | `v0.0.3-spark3.5.2` |
| 3.5.3 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.3.log` | `v0.0.3-spark3.5.3` |
| 3.5.4 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.4.log` | `v0.0.3-spark3.5.4` |
| 3.5.5 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.5.log` | `v0.0.3-spark3.5.5` |
| 3.5.6 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.6.log` | `v0.0.3-spark3.5.6` |
| 3.5.8 | spark-3.5 / JDK 11 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-3.5.8.log` | `v0.0.3-spark3.5.8` |
| 4.0.2 | spark-4.0 / JDK 17 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-4.0.2.log` | `v0.0.3-spark4.0.2` |
| 4.1.1 | spark-4.1 / JDK 17 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-4.1.1.log` | `v0.0.3-spark4.1.1` |
| 4.1.3 | spark-4.1 / JDK 17 | 62 pass, 0 fail | 3 it.* suites aborted (need AWS env) | `/workplace/gromav/spark-s3-connector-update/logs/spark-4.1.3.log` | `v0.0.3-spark4.1.3` |

Summary of all runs: `/workplace/gromav/spark-s3-connector-update/logs/summary.txt`.
Maven exits non-zero for every version because the three `it.*` suites (`S3ConnectorSourceSqsRocksDBItSuite`, `S3ConnectorSourceCrossAccountSqsRocksDBItSuite`, `AsyncSqsClientItSuite`) abort in the constructor with `Env variable TEST_REGION must be defined`. That is the expected abort and was not run. `Tests: failed 0` and 62 succeeded in each log.

Tags (name -> commit): all 17 tags are annotated and point at `82c71c3`. List them with `git -C /workplace/gromav/spark-s3-connector-update/versions tag -l 'v0.0.3-spark*' --format='%(refname:short) %(*objectname:short)'`.

## Shaded jar check (beyond the unit tests)

Unit tests run on the unshaded classpath. For one release per line (3.2.1, 3.3.2, 3.4.1, 3.5.4, 4.0.2, 4.1.1, 4.1.3) the shaded jar was built (`mvn package -DskipTests`, logs `/workplace/gromav/spark-s3-connector-update/logs/package-<version>.log`) and a relocated SQS async client was run against a local fake HTTP endpoint (`/workplace/gromav/spark-s3-connector-update/smoke/Smoke.java`). All seven reached the endpoint and parsed the error reply (`SqsException`, status 400). Results: `/workplace/gromav/spark-s3-connector-update/logs/smoke.txt`. This is not a real SQS call.

## What changed (git log 1684eaf..HEAD)

1. SQS option prefix: `SQS_PREFIX` is now `spark.s3conn.sqs.`. Old `spark.s3conn..sqs.*` keys are still read; the one-dot key wins when both are set. 4 new tests (one-dot, legacy, precedence, CaseInsensitiveMap as Spark passes it).
2. RocksDB `writeBatch.remove` -> `delete`: already in upstream HEAD (PR #12 merged), nothing to do.
3. PR #14 (configurable SQS pool): not merged, left alone (not trivial; it touches the client code).
4. pom: version 0.0.3, six Spark-line profiles, build-helper source dir `src/main/${spark.compat.dir}`, `scala-maven-plugin` declared at top level, shade plugin 3.2.1 -> 3.5.3 (3.2.1 cannot read Java 17 class files), shade includes `netty-codec-base` and `netty-codec-compression`, `module-info.class` excluded.
5. Spark 4.x / Scala 2.13: `.toSeq` on two mutable buffers; `S3SparkUtils.ofRows` finds `Dataset.ofRows` by name (`classic.Dataset$` on Spark 4, `Dataset$` before) and rethrows the real exception; test-scope `aws-java-sdk-bundle` 1.12.791 in the 4.x profiles (Spark 4's hadoop-aws no longer brings SDK v1 for the it.* sources); `JavaConversions` -> `JavaConverters` in `ItTestUtils`.
6. Spark 4.1: `SparkCompat` shim (`src/main/scala-spark-pre41` and `src/main/scala-spark-41`) because `MetadataLog`, `CheckpointFileManager`, `FileStreamSource`, `FileStreamSourceOffset` moved packages. The spark-4.1 profile imports `netty-bom` 4.2.7.Final, because Spark 4.1 needs Netty 4.2 (`NioIoHandler`).
7. rocksdbjni 9.x `setLogger`: the connector source compiles and its RocksDB tests pass against rocksdbjni 9.8.4 (Spark 4.0.2 to 4.1.3), because the jar is built per Spark line. A jar built for Spark 3.x (rocksdbjni 6 to 8) still will not run on Spark 4 (`setLogger(Logger)` has a different signature in 9.x). Use the matching tag.

## Review (separate subagent, transcript `/home/gromav/.kiro/crew/subagents/23724ecaac091569/result.txt`)

| Finding | Severity | Action |
|---|---|---|
| Spark 4.1 shaded jar lacked `netty-codec-base` (Netty 4.2 moved classes), would fail with NoClassDefFoundError at the first SQS call | must-fix | Fixed (shade includes). Verified with the smoke run on 4.1.1 and 4.1.3. |
| `ofRows` reflection wrapped real errors in InvocationTargetException | must-fix | Fixed (rethrow cause). |
| `ofRows` lookup by parameter count only | should-fix | Fixed (checks LogicalPlan type, clear error messages). |
| Option tests did not cover Spark's CaseInsensitiveMap | should-fix | Fixed (test added). |
| `-P spark-x` alone builds a jar with no Scala classes (scala plugin only in `default-test`) | should-fix | Fixed (plugin at top level). |
| `module-info.class` in shaded jar | nit | Fixed (excluded). |
| Duplicate JavaConverters import | nit | Fixed. |
| AWS SDK 2.19.21 is old and ships Netty 4.1.86 (known CVEs fixed in later 4.1); on 4.1 line it is paired with Netty 4.2.7, untested against real SQS | should-fix | Not fixed. Pre-existing and outside the ask; the SDK bump needs the it.* suites. |
| README lacks the profile instructions | nit | Not fixed (README is upstream-facing; MATRIX.md documents it). |
| SDK v1 is end of support (test-scope only) | nit | Not fixed. |

The fixes were re-tested: all 17 versions re-run on 82c71c3 after the fixes (62 pass each), tags created after that.

## Skipped / not checked

- it.* integration suites (3): need AWS env, not run. No real SQS or S3 call was made, so the Netty 4.2 + SDK 2.19.21 pairing (4.1 line) only has the local fake-endpoint smoke test.
- The `-amzn` EMR/Glue jars were not used; builds use open-source Spark from Maven Central.
- Java versions for EMR releases were not looked up (see MATRIX.md).
- Checkpoints written by one Spark line are not guaranteed readable by another: `FileEntry` is Java-serialized into RocksDB and its class moved in Spark 4.1. Not tested.
- Logs live outside git (`/workplace/gromav/spark-s3-connector-update/logs`), as do helper scripts `run-version.sh`, `run-all.sh`, `tag.sh`, `smoke.sh`.
- Baseline test count before my 4 tests was not measured.
