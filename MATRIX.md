# Spark version matrix

Connector version: 0.0.3. One source tree, one Maven profile per Spark line, one local tag per Spark release.

## Version scheme

- Profiles `spark-3.2`, `spark-3.3`, `spark-3.4`, `spark-3.5`, `spark-4.0`, `spark-4.1` set Scala, Hadoop (for the `hadoop-aws` test dependency), Java target and the Spark 4.1 source shim.
- The exact release is chosen with `-Dspark.version=X.Y.Z`. No branch per version.
- Tag: `v0.0.3-spark<spark-version>`, annotated, local only. The tag message holds the build command.
- Build command: `mvn -Pdefault-test,<profile> -Dspark.version=<version> package` (runs the unit tests too) (`default-test` must be listed, because `-P` switches off the default profile).
- Jar: `mvn -Pdefault-test,<profile> -Dspark.version=<version> package` makes the shaded jar `target/spark-streaming-sql-s3-connector-0.0.3-spark<version>.jar` (Maven classifier `spark<version>`, from `${spark.version}`). Use that one with `--jars`. `target/spark-streaming-sql-s3-connector-0.0.3.jar` (no Spark suffix) is the thin jar without the bundled SDK. The tests jar is `spark-streaming-sql-s3-connector-0.0.3-tests-spark<version>.jar`.
- Maven repo: the shaded and tests jars have a per-Spark classifier, but the thin jar, the javadoc jar and the POM are the same GAV (`0.0.3`) for all 17 builds. Deploy those from one build only; a release repo refuses a second upload of them.
- JDK: 11 for 3.x, 17 for 4.x. A jar is built per Spark line and must run on that line (RocksDB, Scala 2.12/2.13 and Spark internals differ).
- EMR `-amzn` builds are tagged by the open-source base version (3.5.4-amzn-0 -> `v0.0.3-spark3.5.4`). The connector uses only open-source Spark classes, but it was not run on the `-amzn` jars.

## Rows (Spark >= 3.2.1)

Spark/Scala/Hadoop/rocksdbjni columns are what the Spark open-source release ships (Maven Central `spark-parent` and `spark-sql` poms, e.g. https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.12/3.5.4/spark-parent_2.12-3.5.4.pom). Release mapping comes from the AWS docs listed below the table.

| Spark | Scala | Hadoop | rocksdbjni | Profile / JDK | EMR releases | Glue | Shaded jar |
|---|---|---|---|---|---|---|---|
| 3.2.1 | 2.12.15 | 3.3.1 | 6.20.3 | spark-3.2 / 11 | 6.7.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.2.1.jar` |
| 3.3.0 | 2.12.15 | 3.3.2 | 6.20.3 | spark-3.3 / 11 | 6.8.x, 6.9.x | 4.0 | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.0.jar` |
| 3.3.1 | 2.12.15 | 3.3.2 | 6.20.3 | spark-3.3 / 11 | 6.10.x | | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.1.jar` |
| 3.3.2 | 2.12.15 | 3.3.2 | 6.20.3 | spark-3.3 / 11 | 6.11.x | | `spark-streaming-sql-s3-connector-0.0.3-spark3.3.2.jar` |
| 3.4.0 | 2.12.17 | 3.3.4 | 7.9.2 | spark-3.4 / 11 | 6.12.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.4.0.jar` |
| 3.4.1 | 2.12.17 | 3.3.4 | 7.9.2 | spark-3.4 / 11 | 6.13.0, 6.14.0, 6.15.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.4.1.jar` |
| 3.5.0 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.0.0, 7.1.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.0.jar` |
| 3.5.1 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.2.0, 7.3.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.1.jar` |
| 3.5.2 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.4.0, 7.5.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.2.jar` |
| 3.5.3 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.6.0, 7.7.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.3.jar` |
| 3.5.4 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.8.0 | 5.0 | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.4.jar` |
| 3.5.5 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.9.0, 7.10.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.5.jar` |
| 3.5.6 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.11.0, 7.12.0, 7.13.0 | 5.1 | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.6.jar` |
| 3.5.8 | 2.12.18 | 3.3.4 | 8.3.2 | spark-3.5 / 11 | 7.14.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark3.5.8.jar` |
| 4.0.2 | 2.13.16 | 3.4.1 | 9.8.4 | spark-4.0 / 17 | emr-spark-8.0.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark4.0.2.jar` |
| 4.1.1 | 2.13.17 | 3.4.2 | 9.8.4 | spark-4.1 / 17 | emr-spark-8.1.0 | 6.0 | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.1.jar` |
| 4.1.3 | 2.13.17 | 3.4.2 | 9.8.4 | spark-4.1 / 17 | emr-spark-8.2.0 | | `spark-streaming-sql-s3-connector-0.0.3-spark4.1.3.jar` |

Not included (below 3.2.1): Glue 3.0 = Spark 3.1.1, EMR 6.6.0 = 3.2.0 and older.
EMR 8.x here means the separate `emr-spark-8.x` release train (the multi-engine `emr-8.0.0` does not exist yet, per the EMR page below).

Java versions from the docs: Glue 4.0 = Java 8, Glue 5.0, 5.1 and 6.0 = Java 17. The EMR Java versions were not looked up. The JDK in the profile column is what the tests ran with (Spark 4 requires 17).

AWS SDK per release (docs): Glue 6.0 uses SDK for Java v2 only. EMR 7.14.0 ships SDK 2.44.5 plus 1.12.797, EMR emr-spark-8.2.0 ships 2.55.8, emr-spark-8.1.0 ships 2.44.5, emr-spark-8.0.0 ships 2.41.32, EMR 6.15.0 ships 2.20.160-amzn-0 plus 1.12.569. Per-release values for the other EMR rows are in the pages below. The connector bundles its own SDK v2, relocated, so it does not depend on the cluster's SDK: 2.19.21 for Spark 3.x, 2.24.6 for Spark 4.0 and 2.29.52 for Spark 4.1 (the SDK that hadoop-aws 3.4.1 / 3.4.2 uses, so the test classpath has one SDK version). Spark's own `aws.java.sdk.version` is 1.11.655 (Spark 3.x and 4.0) and 1.12.681 (4.1), used only by Kinesis modules.

## Sources

- Glue versions: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html (Glue 6.0 = Spark 4.1.1, Scala 2.13.17, Java 17; 5.1 = 3.5.6, Scala 2.12.18, Java 17; 5.0 = 3.5.4, Scala 2.12.18, Java 17; 4.0 = 3.3.0, Java 8; 3.0 = 3.1.1)
- EMR 6.x: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html (Spark column, 6.15.0 back to 6.0.0; Scala 2.12.15 for 6.7+ to 6.15)
- EMR 7.x: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-7.x.html (Spark 3.5.0-amzn-0 for 7.0.0 up to 3.5.8-amzn-0 for 7.14.0; Scala 2.12.18)
- EMR Spark 8.x: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-spark_8.x.html (8.0.0 = Spark 4.0.2-amzn-0, Scala 2.13.16; 8.1.0 = 4.1.1-amzn-0; 8.2.0 = 4.1.3-amzn-0, Scala 2.13.17) and https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark800-release.html (JDK 17 default, Scala 2.13, SDK v1 removed)
- Spark POMs (Scala, Hadoop, rocksdbjni, Spark's own SDK): Maven Central `org/apache/spark/spark-parent_<scala>/<version>/` and `spark-sql_2.12/3.2.1` for rocksdbjni 6.20.3.

## Notes added after review

- A jar built for one Spark line does not run on another line (RocksDB API, Scala 2.12 vs 2.13). Use the tag for your Spark version.
- Spark 4.1 profile imports Netty 4.2.7 (Spark 4.1 needs it), Spark 4.0 profile imports Netty 4.1.118 (Spark 4.0's version); the shaded jar for each line contains that Netty relocated.
- The Spark 4.x jars use SDK 2.24.6 / 2.29.52, which talk to SQS with the JSON protocol (SDK 2.21 and later). The Spark 3.x jars use the older query protocol. Real SQS and its VPC endpoints accept both; old SQS emulators or proxies that filter on `Action=` may not.
