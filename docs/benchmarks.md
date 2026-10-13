# Benchmarks

This page compares request latency and transfer memory between sg-python-api and Autodesk's client
in one measured environment. Use it to see where the two clients differed, not to predict results
on your site. If you have not tried the client yet, start with the
[README quick start](../README.md#quick-start).

## What the results show

- **Latency was mixed.** In these samples, all three upload routes had lower medians with
  sg-python-api, cold and warm, and so did warm downloads. Cold medians were higher for writes,
  schema reads, summaries, and the four-concurrent-read comparison. Warm medians were similar or
  lower except for the encode-heavy batch.
- **Transfer memory was lower in the measured workloads.** sg-python-api's peak process memory grew
  less during the measured upload and download. That observation also depends on payload shape and
  runtime.
- **Your own workload decides tuning.** Measure representative queries and files on your own site
  before choosing concurrency or pool settings.

When you measure, compare end-to-end latency under your expected concurrency, payload sizes, and
site permissions. Keep connection setup, warm requests, and transfer memory distinct, and collect
more samples before drawing conclusions about tail latency. The
[pool and prefetch controls](../COMPATIBILITY.md#configuration-and-observer-extensions) describe
what the client settings change.

## What the results do not show

Only two paired samples were collected per operation and mode, and network and server load were not
controlled. The results therefore do not establish a general performance advantage for either
client, a reliable tail-latency estimate, or suitable tuning for your application.

The results describe one measured build and environment. They do not validate other builds with the
same version number, later changes to the source, or a production deployment. The maintainers
retain the raw report of this measured run outside this repository. A new run writes its own raw
report.

## Measurement conditions

In the tables below, "replacement" means sg-python-api and "official" means Autodesk's client.

| Setting                     | Value                                                                   |
| --------------------------- | ----------------------------------------------------------------------- |
| Replacement package version | `0.1.1`                                                                 |
| Autodesk client version     | `3.10.2`                                                                |
| Python                      | `3.14.6`                                                                |
| Platform                    | macOS, arm64                                                            |
| Samples                     | Two paired rounds per client, operation, and cold/warm mode             |
| Validation                  | Each measured result and the final resource cleanup passed their checks |

## Latency

Medians below are in milliseconds, rounded to one decimal place. Cold and warm samples are shown
separately because initial connection and authentication costs can dominate short requests. With
two observations, a reported nearest-rank p95 would simply be the larger sample; it would not
estimate a reliable tail-latency distribution.

| Operation                           | Replacement cold | Official cold | Replacement warm | Official warm |
| ----------------------------------- | ---------------: | ------------: | ---------------: | ------------: |
| First script-authenticated request  |            366.6 |         358.9 |            119.7 |         124.7 |
| Batch                               |            410.7 |         309.8 |            160.5 |         178.6 |
| Create                              |            407.0 |         328.3 |            175.0 |         172.1 |
| Delete                              |            456.8 |         362.4 |            193.0 |         233.5 |
| Download                            |            602.2 |         605.4 |            336.3 |         377.0 |
| Medium query                        |            362.6 |         371.6 |            120.7 |         126.1 |
| Find one                            |            354.3 |         371.3 |            128.6 |         133.4 |
| Paged query                         |            490.2 |         484.0 |            263.8 |         254.0 |
| Small query                         |            354.1 |         380.7 |            125.6 |         133.8 |
| Four concurrent reads               |            520.6 |         377.5 |            129.6 |         131.1 |
| Decode-heavy read                   |            361.0 |         365.9 |            129.9 |         138.9 |
| Encode-heavy batch                  |            993.3 |         790.8 |            665.0 |         565.4 |
| Entity schema                       |            346.5 |         276.7 |            117.7 |         113.9 |
| Field schema                        |            371.4 |         303.3 |            141.2 |         146.3 |
| First session-authenticated request |            489.3 |         477.0 |            116.3 |         127.6 |
| Summarize                           |            370.7 |         282.8 |            122.2 |         133.5 |
| Update                              |            394.6 |         298.1 |            163.5 |         157.2 |
| Direct upload                       |            982.0 |        1183.9 |            652.6 |         950.1 |
| Generic upload                      |            797.1 |        1128.5 |            596.2 |         814.0 |
| Multipart upload                    |           2389.0 |        3118.5 |           2163.7 |        2745.0 |

## Transfer memory

The table reports the largest increase in process peak resident memory across the two observations,
in MiB rounded to two decimal places. It measures growth above each worker's baseline, not total
memory use or individual Python allocations.

| Workload | Replacement peak increase | Official peak increase |
| -------- | ------------------------: | ---------------------: |
| Download |                  3.34 MiB |               7.31 MiB |
| Upload   |                  1.95 MiB |              24.70 MiB |

The upload measurement used the multipart route. Download memory uses a fresh download-only process
after setup so earlier work does not dominate the process's high-water mark.

## Methodology

Each client runs in a separate Python process. Client order is counterbalanced within paired
rounds, and each operation's result is checked before accepting its elapsed time. Upload and
download checks compare generated-file hashes. Latency samples and memory samples use separate
workers.

For the parallel-read comparison, the official row uses one thread-owned client per persistent
thread, while the replacement row shares one client across four persistent threads. Both issue four
concurrent reads and exclude thread and client setup and closure from timing. These ownership
choices reflect how each client can be used; they are part of the comparison.

A full run writes disposable server records and uploads files. At the default 2 paired rounds, the
workload budget is 18 worker processes, 184 server entities, and 332,032,784 uploaded bytes. A
larger even `BENCH_N`, up to 20, increases the budget to as much as 162 worker processes, 1,840
server entities, and 3,320,327,840 uploaded bytes. Each worker has a one-hour timeout, with
resource recovery and cleanup checks after interruption. See the
[development guide](development.md#artifacts-conformance-and-benchmarks) to measure your own site.
It needs a disposable site and project, and you should account for these costs before a run.

The replacement's upload route labels come from its observed request metadata. The official
client's upload labels come from server capability evidence. A route comparison applies only where
that route was available; it does not establish legacy fallback behavior on other sites.
