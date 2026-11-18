# Result: connector jar named by Spark version (all 17 builds)

Repo `/workplace/gromav/spark-s3-connector-update/versions`, branch `update/spark-versions`.
Code commit: `ca5fcf4` (all 17 tests, jars and tags are on this commit).

## Approach

Maven classifier on the shaded jar, driven by `${spark.version}`. No pom edit per version.

| Jar in `target/` | What it is | Use with `--jars` |
|---|---|---|
| `spark-streaming-sql-s3-connector-0.0.3-spark<version>.jar` | shaded jar, SDK/Netty/metrics relocated to `s3connector.*` | yes |
| `spark-streaming-sql-s3-connector-0.0.3-tests-spark<version>.jar` | test classes (DataConsumer, DataGenerator) | as the app jar in the README examples |
| `spark-streaming-sql-s3-connector-0.0.3.jar` | thin main jar, no bundled deps (99 classes) | no |
| `spark-streaming-sql-s3-connector-0.0.3-javadoc.jar` | scaladoc | no |

pom changes (`pom.xml`):

- maven-shade-plugin 3.5.3: `shadedArtifactAttached=true`, `shadedClassifierName=spark${spark.version}`. The classifier works with shade; no `finalName` was needed. The old `original-*.jar` is gone, and the shaded jar no longer replaces the main jar, so the only file with the Spark suffix is the shaded one.
- `createDependencyReducedPom=false`. Reason (review must-fix): one POM serves every classifier of the GAV. A reduced POM would drop the SDK dependencies and break the thin main jar for Maven users. Classifier users add a `*:*` exclusion (README snippet), because the shaded jar already bundles them.
- maven-jar-plugin `test-jar`: classifier `tests-spark${spark.version}` (tests jar is per Scala line, 2.12 vs 2.13).

Maven repo limit: the thin jar, the javadoc jar and the POM are the same GAV `0.0.3` for all 17 builds. Deploy them from one build only; a release repo refuses a second upload. Documented in MATRIX.md.
`shortcut: one GAV for 17 Spark builds; move Spark into the version (0.0.3-spark<v>, flatten plugin) if a real Maven release is planned.`

## Per-version results (commit `ca5fcf4`)

Command per row: `mvn -B clean package -Dmaven.test.failure.ignore=true -Pdefault-test,<profile> -Dspark.version=<version>` via `/workplace/gromav/spark-s3-connector-update/run-version.sh`. The ignore flag is needed because the 3 `it.*` suites abort without an AWS env. The script then fails unless `failed 0`, `aborted 3`, and all 3 aborted suites are `it.spark.sql.streaming.connector.*`. Summary: `/workplace/gromav/spark-s3-connector-update/logs/summary.txt`. Jar copies: `/workplace/gromav/spark-s3-connector-update/jars/`.

| Spark | Profile / JDK | Unit tests | Shaded jar | Classes (`unzip -l`) | Relocated / connector classes |
|---|---|---|---|---|---|
| 3.2.1 | spark-3.2 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.2.1.jar` | 5177 | 5078 / 55 |
| 3.3.0 | spark-3.3 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.0.jar` | 5177 | 5078 / 55 |
| 3.3.1 | spark-3.3 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.1.jar` | 5177 | 5078 / 55 |
| 3.3.2 | spark-3.3 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.2.jar` | 5177 | 5078 / 55 |
| 3.4.0 | spark-3.4 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.4.0.jar` | 5177 | 5078 / 55 |
| 3.4.1 | spark-3.4 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.4.1.jar` | 5177 | 5078 / 55 |
| 3.5.0 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.0.jar` | 5177 | 5078 / 55 |
| 3.5.1 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.1.jar` | 5177 | 5078 / 55 |
| 3.5.2 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.2.jar` | 5177 | 5078 / 55 |
| 3.5.3 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.3.jar` | 5177 | 5078 / 55 |
| 3.5.4 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.4.jar` | 5177 | 5078 / 55 |
| 3.5.5 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.5.jar` | 5177 | 5078 / 55 |
| 3.5.6 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.6.jar` | 5177 | 5078 / 55 |
| 3.5.8 | spark-3.5 / 11 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.8.jar` | 5177 | 5078 / 55 |
| 4.0.2 | spark-4.0 / 17 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark4.0.2.jar` | 5177 | 5078 / 55 |
| 4.1.1 | spark-4.1 / 17 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.1.jar` | 5374 | 5275 / 55 |
| 4.1.3 | spark-4.1 / 17 | 62 pass, 0 fail | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.3.jar` | 5374 | 5275 / 55 |

Every row: `Suites: completed 7, aborted 3` (the 3 `it.*` suites, need AWS env), BUILD SUCCESS. 4.1 jars are larger because of relocated Netty 4.2.7.
Extra check: the local fake-SQS smoke test (`/workplace/gromav/spark-s3-connector-update/smoke/Smoke.java`) ran on the new jars for 3.2.1, 3.3.2, 3.4.1, 3.5.8, 4.0.2, 4.1.1, 4.1.3: all reached the endpoint and parsed `SqsException` 400 (`/workplace/gromav/spark-s3-connector-update/logs/smoke-classifier.txt`).

## Review (subagent `6b077500343c42cc`, `/home/gromav/.kiro/crew/subagents/6b077500343c42cc/result.txt`)

| Finding | Severity | Action |
|---|---|---|
| Reduced POM becomes the GAV's POM and breaks the thin jar | must-fix | Fixed: `createDependencyReducedPom=false`, README exclusion for classifier users |
| 17 builds publish thin/tests/javadoc/POM to one GAV | must-fix | Tests jar now has `tests-spark<v>` classifier. Thin/javadoc/POM: documented "deploy from one build"; version scheme not changed (shortcut above) |
| `maven.test.failure.ignore` hides real unit failures | should-fix | Fixed in run-version.sh: checks `failed 0`, `aborted 3`, aborted suites are `it.*` |
| `../jars` could keep a stale jar | should-fix | Fixed: delete before build, exit 1 on failure or missing jar |
| tag.sh checks nothing, cannot re-tag | should-fix | Fixed: refuses dirty tree or no passing log, re-creates the tag |
| README bare build vs example name | should-fix | Fixed: says default is Spark 3.2.1, shows the profile command |
| README "without dependencies" wording | should-fix | Reworded |
| MATRIX build command said `test` | nit | Fixed (`package`) |
| README 0.0.1 download link has old naming | nit | Fixed (noted) |
| `netty-nio-client` listed twice in shade includes | nit | Not fixed (pre-existing, harmless) |
| Unquoted vars in scripts | nit | Fixed |

## Tags

All 17 `v0.0.3-spark<version>` tags deleted and re-created locally, annotated, on `ca5fcf4` (were on `82c71c3`). Message names the `package` command and the jar name. Not pushed.
Check: `git -C /workplace/gromav/spark-s3-connector-update/versions tag -l 'v0.0.3-spark*' --format='%(refname:short) %(*objectname:short)'`.

## Push

Branch `update/spark-versions` pushed to `fork` (vgkowski/spark-streaming-sql-s3-connector), fast-forward from `57ac423`. No force, no tags, no PR, not main, origin push stays DISABLED. Pushed head: see the "Pushed head" line at the end (added after the push).

## Jar-name references in other branches (listed, not edited)

feature/pyspark (`/workplace/gromav/spark-s3-connector-update/pyspark`, local branch `pyspark-support`, `63ca1e4`):

| File:line | Reference | Update |
|---|---|---|
| `README.md:27` | `target/spark-streaming-sql-s3-connector-<versiion>.jar` | `-<version>-spark<spark-version>.jar` (take from update/spark-versions) |
| `README.md:166,168,171` | `-<version>.jar`, `-<version>-tests.jar` | `-spark<spark-version>.jar`, `-tests-spark<spark-version>.jar` |
| `README.md:186` | `-<version>-SNAPSHOT.jar`, `-SNAPSHOT-tests.jar` | same as above |
| `python/examples/read_stream.py:3` | `--jars spark-streaming-sql-s3-connector-<version>.jar` | `...-<version>-spark<spark-version>.jar` |
| `python/README.md:22-28` | table column "Scala 2.12 jar for Spark 3.x" | can name the exact jar per Spark release |
| `python/README.md:82-83`, `python/tests/run_matrix.py:44` | `<2.12 jar>`, `/path/x.jar` placeholders | optional: example with the new name |
| `PYSPARK-RESULT.md:51-54` | jars built with `mvn package -Pspark-<line>,default-test` into `.work/jars/` (named `s3-connector-0.0.3-spark-<line>.jar` there) | rebuild gives `...-0.0.3-spark<version>.jar` |

feature/terraform-e2e (`/workplace/gromav/spark-s3-connector-update/e2e`, local branch `e2e-tests`, `fa067ad`):

| File:line | Reference | Update |
|---|---|---|
| `README.md:27,166,168,171,186` | same upstream README lines as above | take from update/spark-versions |
| `e2e_tests/README.md:14` | `--connector-jar target/spark-streaming-sql-s3-connector-0.0.2.jar` | `target/spark-streaming-sql-s3-connector-0.0.3-spark<version>.jar` |
| `e2e_tests/run_e2e.py:4` | docstring `target/spark-streaming-sql-s3-connector-0.0.2.jar` | same |
| `e2e_tests/run_e2e.py:9` | `jars/connector-2.12.jar`, `jars/connector-2.13.jar` | e.g. `spark35=...-0.0.3-spark3.5.x.jar`, `spark40=...-0.0.3-spark4.0.2.jar` |
| `E2E-RESULT.md:72` | `--connector-jar target/spark-streaming-sql-s3-connector-0.0.2.jar` | same |
| `e2e_tests/conftest.py:71-73` | uploads as `connector.jar` (fixed S3 key) | no change needed |

Note: any script that picks `target/spark-streaming-sql-s3-connector-0.0.3.jar` after a build now gets the thin jar (no SDK). It must use the `-spark<version>` jar.

## Skipped / risk

- No install/deploy to a Maven repo was tested; POM behaviour is from the plugin docs and the built files.
- it.* suites not run (need AWS). The 4.1 Netty 4.2 + SDK 2.19.21 pair is only smoke-tested locally.
- `target/` in this repo was built by another process at 21:25 (Scala 2.13) before my run; `target/` now holds the 4.1.3 build.
