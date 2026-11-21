---
title: "Remote Shuffle Partition Read Measurement"
description: "Measured latency, throughput, and memory for the remote shuffle partition reader"
---

# Pangrova Remote Shuffle Partition Read Measurement

Measured on 2026-11-14 on macOS 13.7.8, Apple M2, 8 cores, 16 GiB memory, and
Corretto 17. The experiment compared the Java reader at commit
`5d28b406a8b72522fec2025df5d2196dfb49141b` with the working-tree reader. The
latter performs the published upper-bound check as each chunk arrives and verifies
the Coordinator receipt and content digest before acknowledging completion to the
consumer.

## Measurement Scope

Each JVM created an independent PostgreSQL database, MinIO bucket, and mTLS
shuffle service, registered one producer, sent four Arrow IPC batches through the
production write path, and then published one partition through the Coordinator's
remote implementation. Each batch contained 137,000 ascending BIGINT values. Every
read covered the same 548,000 rows and 4,453,760 bytes. The test fixture's mTLS
relay observed the frames emitted by the production service. Both readers shared
the connection pool, committed partition, consumer identity, task authorization,
and server configuration. The baseline reader, partition request, and
value-verification classes were extracted from Git objects and prefixed for the
three types so that the same JVM could load them. The reproduction script
additionally added imports for current artifacts to the historical exception
classes; the original read, write, and verification logic was preserved.

Each JVM warmed up six pairs and then recorded 32 pairs. Pairs alternated between
"baseline, current" and "current, baseline" ordering, across three independent
JVMs. Timing used a monotonic clock, began when the connection was borrowed, and
covered opening the reader, reading and verifying the terminal state, closing the
reader, and returning the connection. Every sample was checked for a valid data
byte count and a genuine terminal frame. Recorded protobuf bytes include only the
ReadPartition response body; TCP, TLS, and HTTP/2 overhead was not measured.

## Measured Results

| JVM | Reader | Samples | Median ms | Sample P95 ms | Throughput MiB/s |
|---|---|---:|---:|---:|---:|
| 1 | Baseline | 32 | 166.68 | 194.64 | 24.82 |
| 1 | Current | 32 | 164.19 | 181.41 | 25.00 |
| 2 | Baseline | 32 | 189.25 | 279.88 | 20.99 |
| 2 | Current | 32 | 189.65 | 315.34 | 20.85 |
| 3 | Baseline | 32 | 155.07 | 176.46 | 26.59 |
| 3 | Current | 32 | 153.99 | 172.65 | 27.16 |
| Combined | Baseline | 96 | 166.68 | 216.37 | 23.90 |
| Combined | Current | 96 | 164.65 | 237.55 | 24.04 |

Throughput divides total valid payload by the sum of all sample durations for that
reader; quantiles use R7. Every read observed eight data frames and one terminal
frame, totaling 4,454,625 protobuf response bytes. Process peak RSS for the three
JVMs was 697,466,880, 722,599,936, and 828,014,592 bytes, respectively; these
figures include both readers plus the co-located service and fixture and cannot be
attributed to a single implementation. The raw 192 samples are in the
per-read data.

An independent Maven build was running on the same machine at the time. In the
second JVM, both implementations showed higher latency than in the other two
JVMs. After combining, the current implementation's median decreased by 2.03 ms
and sample P95 increased by 21.18 ms. These observations used a non-dedicated
host and provide neither whole-SQL-query latency nor a production tail-latency
baseline, so they cannot establish a performance threshold. JNI copy volume,
cluster link bytes, read/write backpressure, and native peak require separate
metrics and load.

## Reproduction

Prepare JDK 17, the SDK and Docker required by the repository, and the Maven test
classpath that passes the external shuffle integration test for this version. That
classpath must include the integration test's test-classes, the Worker test
fixture, all relevant production classes, test resources, and Maven dependencies.
The actual paths used by the JVM can be extracted from the Surefire XML of a
freshly completed `NativeExchangeTransportExternalIntegrationTest`. Run the
commands below when sources and compiled output agree; the output directory must
not already exist.

```sh
python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET
report = next(Path("pangrova-testing").glob("**/target/surefire-reports/TEST-*NativeExchangeTransportExternalIntegrationTest.xml"))
classpath = ET.parse(report).find(".//property[@name='java.class.path']").get("value")
Path("/tmp/pangrova-shuffle-classpath.txt").write_text(classpath)
PY
python3 pangrova-testing/pangrova-testing-benchmark/scripts/shuffle_read_comparison.py \
  --classpath-file /tmp/pangrova-shuffle-classpath.txt --output /tmp/pangrova-shuffle-read-results
```

This run used a classpath captured from a test JVM that completed the JNI
handshake and compiled the latest split Provider, Server, Worker, and real fixture
in a separate directory to isolate parallel module refactoring. G1 heap size was
512–1024 MiB. The four batches used identical Arrow bytes; the single-batch
SHA-256 is
`d3adaf21c8c463a2932cba39d17b7e03e78dcde654801980eac4b9b8865e28b2`. The raw CSV
SHA-256 is
`2760a5d93141b855e0cc7608c031a4dd542f45a14947c9051b24676e5b5a9267`.

| Working-tree source at measurement | SHA-256 |
|---|---|
| RemoteShuffleExchangeSource.java | `cdeead8f1d4eae15100395b145dab066fc8b9b46e7237a058585402f278d9707` |
| PangrovaPartitionRead.java | `c08b7e3a6b7dcf1cc1befb27816db7c5a2d25a159bc6fffb68afb7606423a02e` |
| StreamingBatchChunkValidator.java | `fad5728ab9ff8976b35ede82779f35175bbb9efdb8b507e0446a5321f9549219` |
