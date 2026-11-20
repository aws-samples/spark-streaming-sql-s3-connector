# Connector use cases and the e2e test that covers each

Source of truth: `README.md` and `src/main/scala/.../S3ConnectorSourceOptions.scala`,
`S3ConnectorSource.scala`, `S3ConnectorFileValidator.scala`, `S3ConnectorFileCache.scala`,
`client/AsyncSqsClientImpl.scala`, `metadataLog/RocksDBS3SourceLog.scala` of connector 0.0.3.

Every test runs the connector jar on EMR Serverless (one application per Spark release label)
against a real S3 bucket and SQS queue made by `terraform/`. Each scenario has its own S3 prefix
(`input/<scenario>/`), its own queue and its own DLQ, so tests do not see each other's messages.

Test ids are `e2e_tests/<file>::<test>`.

## Read path and file formats

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-01 | `fileFormat=csv` | Files from SQS events are read with Spark's csv source | `test_formats.py::test_file_format[csv]` |
| UC-02 | `fileFormat=json` | same, json | `test_formats.py::test_file_format[json]` |
| UC-03 | `fileFormat=parquet` | same, parquet | `test_formats.py::test_file_format[parquet]` |
| UC-04 | `fileFormat=orc` | same, orc | `test_formats.py::test_file_format[orc]` |
| UC-05 | `fileFormat=text` | same, text (single `value` column) | `test_formats.py::test_file_format[text]` |
| UC-06 | Format options pass through (`header=true` for csv) | `options` map is handed to `DataSource` | `test_formats.py::test_csv_header_option` |
| UC-07 | Schema mismatch with `mode=DROPMALFORMED` | bad rows dropped, good rows kept | `test_formats.py::test_malformed_rows_dropped` |
| UC-08 | Object keys with `+`, `=` and `,` | event key is URL-decoded before read | `test_formats.py::test_special_characters_in_key[special_keys-...]` |
| UC-08b | Object key with a space | The file is read (the decoded key is passed to Spark as a Hadoop `Path`, not a `URI`) | `test_formats.py::test_special_characters_in_key[space_key-...]` |
| UC-09 | User schema is required | `sourceSchema` fails without a schema | `test_failures.py::test_invalid_configuration[no_schema]` |
| UC-10 | `queueType=SQS` explicit | accepted, same as default | `test_formats.py::test_file_format[json]` sets it |

## File selection

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-11 | `partitionColumns` + `basePath`, two levels | partition values come from the path | `test_selection.py::test_partition_columns` |
| UC-12 | `maxFilesPerTrigger=N` | at most N files per micro-batch | `test_selection.py::test_max_files_per_trigger_limit` |
| UC-13 | `maxFilesPerTrigger=-1` (unlimited) | `-1` becomes `None`, no per-batch cap | `test_selection.py::test_max_files_per_trigger_unlimited` |
| UC-14 | `pathGlobFilter` | non-matching files skipped, their messages deleted | `test_selection.py::test_path_glob_filter` |
| UC-15 | `maxFileAge`: file older than the window | `FileExpired`, not read, message deleted | `test_selection.py::test_max_file_age_and_late_files` |
| UC-16 | Late file inside the `maxFileAge` window | read normally | `test_selection.py::test_max_file_age_and_late_files` |

## SQS messages

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-17 | Every consumed message is deleted (queue and DLQ empty after a run) | `handleProcessedMessageBatch` runs when a batch is planned (after the RocksDB add, before the batch runs) | asserted in every positive test via `assert_drained`; tests that check "not read" wait a few triggers after the drain before stopping |
| UC-18 | Same object uploaded twice (new event, same path) | `PersistedInMetadataLog`/`ExistInCacheProcessed`, read once | `test_messages.py::test_duplicate_files_and_messages` |
| UC-19 | Same SQS message delivered twice | read once | `test_messages.py::test_duplicate_files_and_messages` |
| UC-20 | Non-create events (`ObjectRemoved`, `s3:TestEvent`) and malformed bodies | parsed to `None`, deleted, stream keeps running | `test_messages.py::test_non_create_and_malformed_messages` |
| UC-21 | `sqs.keepMessageForConsumerError=true` | invalid-file message kept, ends in DLQ after `maxReceiveCount` | `test_messages.py::test_keep_message_on_error[double_dot]` and `[single_dot]` |
| UC-22 | `sqs.keepMessageForConsumerError=false` (default) | invalid-file message deleted, DLQ empty | `test_selection.py::test_path_glob_filter` |
| UC-23 | `sqs.*` keys, legacy two-dot form `spark.s3conn..sqs.*` (releases up to 0.0.2, still accepted) | values applied | `test_messages.py::test_keep_message_on_error[double_dot]`, `test_failures.py::test_invalid_configuration[*_double_dot]` |
| UC-24 | `sqs.*` keys, one-dot form `spark.s3conn.sqs.*` (documented form) | values applied | `test_messages.py::test_keep_message_on_error[single_dot]`, `test_failures.py::test_invalid_configuration[*_single_dot]`. These fail on jars up to 0.0.2, which read only the two-dot keys. Marker `sqs_single_dot`. |
| UC-25 | `sqs.longPollingWaitTimeSeconds`, `sqs.visibilityTimeoutSeconds`, `sqs.maxConcurrency`, `sqs.maxRetries`, `queueFetchWaitTimeoutSeconds` set | accepted; out-of-range values rejected | set in every positive run (`harness/events.py::base_options`: poll 2 s, visibility 30 s, concurrency 5, retries 3, fetch wait 4 s); range checks in `test_failures.py::test_invalid_configuration` |
| UC-26 | S3 -> SNS -> SQS fan-out to two applications (raw message delivery) | each queue's stream reads the file | `test_messages.py::test_sns_fanout` |

## Queue location

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-27 | Queue in the same region and account | default path | all positive tests |
| UC-28 | Queue in another region (`queueRegion`) | SQS client built for `queueRegion` | `test_remote.py::test_remote_source` (remote module, other region) |
| UC-29 | S3 bucket and SQS queue in another account | bucket and queue policies grant the job role | `test_remote.py::test_remote_source` (remote module with `assume_role_arn`) |
| UC-30 | Wrong `queueRegion` | receive errors are only logged; stream stays up, reads nothing | `test_failures.py::test_invalid_configuration[wrong_queue_region]` (own queue with one pending file: no rows, message still queued) |

## Reprocessing

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-31 | `NoReprocess` (no start/end ids) | normal run | all positive tests |
| UC-32 | `DryRun` (start+end, `reprocessDryRun` default `true`) | lists files, exits with `S3ConnectorReprocessDryRunException` "N batches, M files" | `test_state.py::test_reprocess` |
| UC-33 | `InAction` (`reprocessDryRun=false`) | re-emits the files of batches start..end as new batches, writes `reprocess.lock`, does not read new SQS messages | `test_state.py::test_reprocess` |
| UC-34 | Second `InAction` with the lock present | `S3ConnectorReprocessLockExistsException` | `test_state.py::test_reprocess` |
| UC-35 | Normal restart after reprocess | new messages read again | `test_state.py::test_reprocess` |
| UC-36 | `reprocessStartBatchId > reprocessEndBatchId`, negative ids | `IllegalArgumentException` | `test_failures.py::test_invalid_configuration[reprocess_*]` |
| UC-37 | Only one of start/end given | treated as `NoReprocess` | `test_failures.py::test_invalid_configuration[reprocess_start_only]` (own queue with one pending file: no exception, the file is read) |

## Metadata log, checkpoint and recovery

| ID | Use case | Code behaviour | Test |
|---|---|---|---|
| UC-38 | RocksDB metadata log in `<checkpoint>/sources/0` (`<version>.zip`, `SSTs/`) | one version per batch, `numKeys` grows | `test_state.py::test_checkpoint_recovery` |
| UC-39 | Graceful stop and restart from the same checkpoint | batch ids continue, only new files read | `test_state.py::test_checkpoint_recovery` |
| UC-40 | Re-upload of a processed file after restart | RocksDB dedupe across restarts, not read again | `test_state.py::test_checkpoint_recovery` |
| UC-41 | Driver crash after a batch's sink write, before its commit (`crash_after_batch` in the job), then restart | offset N written, commit N missing; restart replays batch N with the same files; resumed output has each row once | `test_state.py::test_checkpoint_recovery` |
| UC-42 | Query uses its own state store (HDFS default and RocksDB provider) next to the connector's RocksDB | stateful aggregation works | `test_state.py::test_stateful_query[hdfs]`, `[rocksdb]` |

## Failure cases (one EMR job runs all of them, see `job/stream_job.py` mode `cases`)

| ID | Use case | Expected | Test |
|---|---|---|---|
| UC-43 | `fileFormat` missing | `IllegalArgumentException` | `test_failures.py::test_invalid_configuration[no_file_format]` |
| UC-44 | `queueUrl` missing | `IllegalArgumentException` | `[no_queue_url]` |
| UC-45 | `queueRegion` missing | `IllegalArgumentException` | `[no_queue_region]` |
| UC-46 | `queueType` not SQS | `S3ConnectorUnsupportedQueueTypeException` | `[bad_queue_type]` |
| UC-47 | `sqs.longPollingWaitTimeSeconds=30` (max 20), both key forms | `IllegalArgumentException` | `[long_poll_out_of_range_double_dot]`, `[..._single_dot]` |
| UC-48 | `sqs.maxRetries=0`, `sqs.maxConcurrency=0`, `sqs.visibilityTimeoutSeconds=0`, both key forms | `IllegalArgumentException` | `[max_retries_zero_*]`, `[max_concurrency_zero_*]`, `[visibility_zero_*]` |
| UC-49 | `sqs.keepMessageForConsumerError=maybe`, both key forms | `IllegalArgumentException` | `[keep_not_boolean_*]` |
| UC-50 | `queueFetchWaitTimeoutSeconds=0` | `IllegalArgumentException` | `[fetch_wait_zero]` |
| UC-51 | `reprocessDryRun=maybe` | `IllegalArgumentException` | `[reprocess_dry_run_not_boolean]` |

## PySpark helper package (`python/`, marker `pyspark`, `run_e2e.py --suite pyspark`)

The job is `job/pyspark_job.py` with `python/src/spark_s3_connector` and `job/stream_job.py` on
`--py-files` (a zip built by `harness/pyfiles.py`); every source is built with `read_stream(spark, schema, **kwargs)`.

| ID | Use case | Expected | Test |
|---|---|---|---|
| UC-52 | Plain read of CSV, JSON, Parquet, ORC, text | every row once, queue drained, batch committed | `test_pyspark.py::test_pyspark_file_format[csv\|json\|parquet\|orc\|text]` |
| UC-53 | `partition_columns=["p1", "p2"]`, `base_path` | partition values as columns | `test_pyspark.py::test_pyspark_partition_columns` |
| UC-54 | `max_files_per_trigger=1` and `extra_options={"header": "true"}` | one file per batch; header line not a row | `test_pyspark.py::test_pyspark_option_keys_reach_connector` |
| UC-55 | Valid kwargs read; bad kwargs raise in Python before Spark (`ValueError` / `TypeError`); a bad one-dot `sqs.maxRetries` passed through `extra_options` is rejected by the Scala parser | as listed | `test_pyspark.py::test_pyspark_option_checks[...]` |
| UC-56 | Stateful aggregation with the default (HDFS) state store | running counts correct | `test_pyspark.py::test_pyspark_stateful_query_hdfs` |

## Not covered, and why

- Mixing both `sqs.*` spellings in one query: the one-dot key wins (covered by the connector's unit tests); each E2E test uses one spelling only.
- `basePath` as a filter (files outside it are skipped and their messages deleted): covered by the connector's unit tests, not by an E2E case.

- Spark versions older than the release labels you pass: the same suite runs per label; pick labels in `run_e2e.py --release-label`.
- Throughput and SQS pool sizing under load: out of scope for functional e2e (`src/test/scala/pt` is the load tool).
- `queueType` other than SQS: the code supports only SQS (UC-46 covers the rejection).
