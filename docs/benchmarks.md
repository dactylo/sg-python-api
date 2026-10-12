# Benchmarks

This page compares request latency and transfer memory between sg-python-api and Autodesk's client
in one measured environment. Use it to see where the two clients differed, not to predict results
on your site. If you have not tried the client yet, start with the
[README quick start](../README.md#quick-start).

## What the results show

- **Latency was mixed.** In these samples, downloads and all three upload routes had lower medians
  with sg-python-api. Most queries, writes, and schema reads had similar or higher medians,
  including the four-concurrent-read comparison.
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
| Replacement package version | `0.0.1`                                                                 |
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
| First script-authenticated request  |            359.4 |         364.1 |            124.2 |         128.9 |
| Batch                               |            419.7 |         339.2 |            202.3 |         199.9 |
| Create                              |            431.2 |         380.3 |            228.3 |         185.2 |
| Delete                              |            431.9 |         346.0 |            183.6 |         173.3 |
| Download                            |            603.2 |         767.6 |            340.1 |         450.4 |
| Medium query                        |            384.2 |         366.1 |            199.0 |         135.8 |
| Find one                            |            401.2 |         360.3 |            134.3 |         119.1 |
| Paged query                         |            551.0 |         499.9 |            263.8 |         256.5 |
| Small query                         |            362.4 |         360.6 |            138.6 |         129.5 |
| Four concurrent reads               |            551.5 |         387.7 |            191.1 |         130.4 |
| Decode-heavy read                   |            395.4 |         371.3 |            134.9 |         130.4 |
| Encode-heavy batch                  |            924.8 |         809.8 |            739.7 |         628.8 |
| Entity schema                       |            360.6 |         341.5 |            140.4 |         189.8 |
| Field schema                        |            401.6 |         304.8 |            148.8 |         143.8 |
| First session-authenticated request |            486.5 |         462.2 |            119.7 |         120.5 |
| Summarize                           |            374.6 |         310.1 |            132.4 |         132.8 |
| Update                              |            384.3 |         349.2 |            147.9 |         156.7 |
| Direct upload                       |           1026.3 |        1242.3 |            655.8 |        1006.6 |
| Generic upload                      |            782.4 |        1169.4 |            577.0 |         743.4 |
| Multipart upload                    |           3016.3 |        3423.4 |           2037.6 |        2906.0 |

## Transfer memory

The table reports the largest increase in process peak resident memory across the two observations,
in MiB rounded to two decimal places. It measures growth above each worker's baseline, not total
memory use or individual Python allocations.

| Workload | Replacement peak increase | Official peak increase |
| -------- | ------------------------: | ---------------------: |
| Download |                  3.69 MiB |               6.63 MiB |
| Upload   |                  0.94 MiB |              24.50 MiB |

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
