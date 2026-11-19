# Startup and memory investigation, 2026-11-19

Steady-state writes now measure 11.62 MB per 15 minutes,
versus 222.09 MB for baseline `791b690` and
485.72 MB for the previous startup/read fix. The cause
was competing transcript summaries repeatedly overwriting the same session row.
Commit `3933b03` consolidates those summaries while preserving every source's
usage and commit-before-acknowledgement ordering. The first status response
remains fast at 0.390 seconds.

The original 15-minute measurements below compare deployed `791b690` with the
previous local release. The follow-up after those findings measures startup and
settling separately from a full warmed 15-minute window. All runs are isolated
measurements on nixed, not measurements of a restarted system service.

| Metric | Deployed 791b690 | Previous fix, original run |
|---|---:|---:|
| Launch to first status response | 81.098 s | 0.347 s |
| Status median / p95 | 294.9 / 338.5 ms | 157.0 / 188.9 ms |
| Peak daemon RSS | 439.6 MiB | 268.5 MiB |
| CPU time | 334.79 s | 195.00 s |
| Physical reads | 5.667 GB | 5.753 GB |
| Logical reads, rchar | 166.294 GB | 24.572 GB |
| Physical writes | 211.1 MB | 812.9 MB |

The first fixed status response included `warming: true`. The flag cleared by
100.8 seconds. Hooks were probed every ten seconds during the fixed run,
including startup; their median was 11.65 ms and maximum was
56.15 ms. The baseline's auth-status route also waited roughly 80 seconds,
which establishes that startup blocked HTTP serving rather than only the status
query.

Physical reads remain dominated by one cold scan of the 2.6 GiB Claude and
2.8 GiB Codex transcript directories. After the first 180 seconds, the baseline
read another 7.0 MB physically and the fixed daemon read
3.7 MB. Logical reads include SQLite reads served by the page cache;
they should not be presented as physical disk traffic. Systemd includes service children and charged file cache. A read-only check of
the running service found a Codex app-server child at about 92 MiB RSS and
187 MiB of charged file cache. The earlier service's
2 GiB cgroup peak was not reproduced as daemon RSS: this baseline peaked at 439.6 MiB.

The implementation addresses these measured or identified costs:

- Startup bound the socket before `Store::open` finished. Its correlated
  reconciliation queries repeatedly searched usage history for the latest
  account/window snapshot without matching indexes. The baseline consumed
  about 81 CPU seconds and 147 GB of logical reads before serving. Composite
  indexes preserve the existing query results. Database initialization now runs
  on a blocking worker before socket creation. See
  `crates/uw-store/src/lib.rs:206` and `crates/uw-daemon/src/lib.rs:3244`.
- Codex discovery did synchronous directory/file parsing while holding its
  cache mutex on a Tokio worker. Changed rollouts were fully reparsed and then
  sliced by the previous record count; rewriting one record could therefore
  lose usage. Claude's discovery and quota checks also reparsed changed files.
  The common JSONL cursor now resumes from a saved byte offset and resets on
  replacement, shrinkage, same-size modification, or changed head/tail bytes.
  Scanning, pruning and potentially contended acknowledgement locks run on
  blocking workers. See `crates/uw-adapters/src/jsonl.rs:44`,
  `crates/uw-adapters/src/codex.rs:510` and
  `crates/uw-adapters/src/claude_code.rs:920`.
- Both discovery caches held and replayed historical token vectors. Activity
  checks also loaded entire transcripts; Codex retained full tool-call payloads
  while reconstructing activity. Cached summaries now retain activity state and
  uncommitted usage. A committed prefix is acknowledged for its session and
  path, then released. A cancelled write replays safely through SQLite's existing
  uniqueness constraint. A delayed acknowledgement cannot clear a replacement
  owner's records, even when the token records are identical. See
  `crates/uw-daemon/src/lib.rs:963` and adapter acknowledgement methods.
- Status loaded every session's complete token history and held the writer
  mutex during aggregation. It now uses an independent read-only WAL connection
  and only the 24-hour token history needed by the existing display and activity
  rules. Provider usage history and policy formulas remain unchanged. See
  `crates/uw-web/src/lib.rs:583` and `crates/uw-store/src/lib.rs:672`.
- Policy passes rebuilt the same account/window analysis for each session.
  They now reuse the calculation within that pass, including the near-limit
  advisory path. See `crates/uw-daemon/src/lib.rs:2370`. Unsupported usage polls
  now log at DEBUG; other fetch failures still log at WARN.

The existing auth integration test passed: the Unix socket remains mode 0600
and skips login, while TCP routes and internal aliases require login. Hook
backend failures retain the existing best-effort fallback. Delivery claim logic
and policy formulas were not changed.

The scanner tests state why each case matters. They cover append deltas,
unchanged files, incomplete records completed later, truncation and regrowth,
rotation, replacement with identical bytes and mtime, rewritten records, stale
acknowledgements, and a changed session owner. A production-path test cancels
observation both before and after SQLite commit and verifies exactly one stored
record after retry through the compaction fallback wrapper. Activity tests prove
that a partially appended tool call is Unknown until complete, then Busy.

Boat `bx_q2mxq7m9`, default size, ran `cargo test --workspace -j2` with
459 passing tests, no failures or ignored tests, and
`cargo clippy --workspace --all-targets -j2 -- -D warnings`. Six deliberate
mutations were caught: stale-prefix acknowledgement, wrong-owner
acknowledgement, missing fallback forwarding, writer-locked status, partial activity treated as complete, and treating a delayed newline as a
malformed activity record. All mutations were restored. The sandbox was
stopped after downloading the logs. No test suite ran locally.

Each measured daemon used a fresh SQLite backup of the same real database seed,
a private scratch directory, a different Unix socket and endpoint file, and
`UW_LISTEN_ADDR=127.0.0.1:0`. Bubblewrap mounted the host read-only except that
scratch directory. A separate network namespace and an unavailable executable
PATH prevented copied queues from delivering compactions or resumes to live
sessions. Provider HTTP/app-server calls therefore failed in both runs and T3
transport was disabled. This comparison covers native transcript scanning,
SQLite and API serving; it does not measure live provider/T3 transport behavior.
The live service remained running under its original PID, 3340986. Nothing was pushed,
restarted, rebuilt through NixOS, or written to the real usage database.

The harness sampled `/proc/<pid>/status`, `/proc/<pid>/stat` and
`/proc/<pid>/io` every 250 ms for the first two minutes, then every second.
Status/auth-status requests ran every ten seconds. Status median/p95 exclude
those first two minutes. VmHWM supplies peak RSS, including brief peaks between
samples. Both runs lasted over 900 seconds. Page caches were not flushed on this
shared host, transcript writers remained active, and the local release build used Rust 1.99.1 while the deployed binary's
toolchain was not verified. Treat small physical-read differences as
uncontrolled, not an improvement or regression established by this experiment.

Offsets are in memory, so each daemon start still performs one full background
scan. They are not a persisted ingestion checkpoint. The cursor assumes normal
append-only transcript writing and ordinary rotation/truncation. An arbitrary
in-place historical rewrite which regrows before a poll while preserving its
inode and both sampled 256-byte boundaries can resemble an append; detecting
that requires full-prefix verification or a writer generation marker. This is
a limitation, not a tested guarantee against arbitrary historical editing.

Raw samples, request timings, the runnable measurement harness, JSON summaries,
the memory chart and Boat logs are in
`/home/mini/Documents/repos/usagewindow.research/startup-memory/`. The benchmark
binary is retained there as `uw-daemon-fixed`. Both scratch databases passed SQLite integrity checking and retained every token-usage row present in the seed database.

The profiled executable is retained as `uw-daemon-profiled`, SHA-256
`c33af8d27971cb9db28eb85aacf0dcb379f73824da1c941bd37c2f05faffdbc8`.
It includes the performance changes and owner-scoped acknowledgements. The
source then received equivalent Clippy guard formatting and a small activity
fix which ignores a delayed newline after a complete EOF record. The latter
was covered by the focused activity tests on both adapters; the 15-minute
profile was not restarted for that guard.

Physical writes increased 3.85 times in this original combined startup/steady
window. The follow-up below separates warmed writes and removes repeated
source-summary updates. RSS grew another
67.0 MiB after minute three, compared with
199.9 MiB in the baseline. This experiment establishes
a lower 15-minute peak, not a proven long-run plateau or a reproduced/fixed
2 GiB service-cgroup peak.

Local implementation commits, not pushed:

- `f8cc40f` Serve stored status promptly during daemon warmup
- `1dc4d1b` Scan transcript appends off the runtime and acknowledge committed usage
- `05f694e` Reuse account window analysis within each policy pass
- `e01c94a` Guard incremental scans against changed owners and delayed newlines
- `3933b03` Consolidate discovery sources before persisting session summaries

## Steady-state write investigation

Each binary started with a fresh copy of the same 85.6 MB seed backup and an
empty in-memory discovery cache. The first 180 seconds include database setup,
the initial background full scan, and settling. This common allowance is
conservative: baseline has no warming flag, so its exact first-scan completion
is not separately exposed. For the other binaries the flag cleared before
180 seconds. The steady window starts only after that allowance and a successful
non-warming status response, and then lasts 900 seconds. Startup/settling bytes
are excluded from the steady-state rate.

| Binary | Startup/settling writes, first 3 min | Steady writes per 15 min | Peak WAL size | First status |
|---|---:|---:|---:|---:|
| Baseline `791b690` | 35.37 MB | 222.09 MB | 4.128 MB | 81.364 s |
| Previous fix `022ca9b` | 151.90 MB | 485.72 MB | 4.141 MB | 0.389 s |
| Now `3933b03` | 12.98 MB | 11.62 MB | 4.145 MB | 0.390 s |

The new steady-state rate is 94.8% below the newly measured baseline and
97.6% below the previous fix. It is also below the original 211 MB target.
Writes use Linux `/proc/<pid>/io` `write_bytes`, with decimal MB. Actual sampled
steady windows were 899.516, 900.630, and 900.598 seconds;
the table normalizes their byte deltas to 900 seconds. This repeat does not
reproduce the original 3.85 factor exactly. Transcripts remained live and the
host/page cache was shared, so that factor is not a stable workload constant.

| Binary | Warming first observed clear | Warm status median / p95 | Logical reads, complete 18-minute run | Peak daemon RSS |
|---|---:|---:|---:|---:|
| Baseline | Not exposed | 307.6 / 339.7 ms | 169.064 GB | 409.4 MiB |
| Previous fix | 101.05 s | 157.4 / 184.7 ms | 22.969 GB | 245.5 MiB |
| Now | 30.61 s | 152.4 / 183.4 ms | 20.953 GB | 262.8 MiB |

Logical reads are `rchar`, not disk reads. The startup and read gains remain;
these measurements still do not establish a long-run RSS plateau.

### Where the writes came from

Claude discovery returned one summary per transcript path. On this host,
649 sidechain files across 14 parent sessions all carried their parent's
`sessionId`. Their differing paths repeatedly overwrote the same parent row.
The next pass repeated those changes even when every transcript was unchanged. See `crates/uw-adapters/src/claude_code.rs:1110`, the per-source
loop at `022ca9b:crates/uw-daemon/src/lib.rs:958`, and the unconditional conflict
update in `crates/uw-store/src/lib.rs:551`.

WAL frame-page attribution confirms that this was a session-table write
hotspot. The baseline's mid-run sample contained 453 session-table frames out
of 481 frames in its current WAL generation. The previous fix's final sample
contained 794 session frames out of 828; the new sample contained 4 out of 29.
The complete table breakdown is in `writes-comparison.json`.
Attribution checks WAL salts so stale frames left behind by recycling are not
mistaken for current-generation frames.

Discovery now sorts sources by session ID, last-seen timestamp, and path, then
persists one consolidated summary per ID. It retains the earliest known
first-seen time and combines all pending token records. After the combined
records commit, it acknowledges each original path with that path's original
record prefix. Cancellation before or after commit therefore retries safely
through the existing uniqueness constraint. See
`crates/uw-daemon/src/lib.rs:952` and `crates/uw-daemon/src/lib.rs:1027`.

A latent undated-transcript bug was also fixed. No undated sessions occurred
in these three runs, so it did not cause their measured regression. An absent
last-seen timestamp became a fresh `now` on every observation. Such sessions now reuse
the stored fallback timestamp until dated evidence arrives, retaining the
missing-timestamp marker. See `crates/uw-daemon/src/lib.rs:731`.

The production-path regression test reorders competing sources, replays a
duplicate token record, and repeats discovery. It observes zero WAL bytes
on unchanged passes, while preserving 12 + 24 = 36 unique tokens and
acknowledging every source. The initial insert is a positive control for the
WAL probe. This also checks the duplicate-insert hypothesis against the actual
schema: ignored duplicates do not produce WAL frames here. No token uniqueness
constraint or durability setting changed.

A separate three-transcript fixture with a one-second observation cadence
wrote 1,015,808 bytes over ten warmed seconds in both old binaries and 409,600
bytes in the new binary. The syscall traces identify WAL writes and fsync calls;
the fixture still includes legitimate provider-attempt updates. This small
reproduction supports the persistence change, not the old 3.85 factor.

The isolated runs generated no provider cache JSON files. Provider calls fail
in the network namespace, as in the original comparison. The discovery caches
are in memory. There is no VACUUM path; the hourly maintenance job prunes old
usage at startup and does not run again inside these 18-minute measurements.
Those paths do not explain the warmed write hotspot in this experiment.

### WAL and checkpoint behaviour

| Approximate time after launch | Baseline WAL | Previous-fix WAL | New WAL |
|---|---:|---:|---:|
| 0 min | 0.000 MB | 3.926 MB | 3.939 MB |
| 3 min | 4.120 MB | 4.120 MB | 4.145 MB |
| 5 min | 4.120 MB | 4.120 MB | 4.145 MB |
| 10 min | 4.120 MB | 4.141 MB | 4.145 MB |
| 15 min | 4.128 MB | 4.141 MB | 4.145 MB |
| 18 min | 4.128 MB | 4.141 MB | 4.145 MB |

The allocated WAL high-water size stays near 4.1 MB; recycling reuses that
space. The sampled frame counters show repeated recycle cycles, not an
indefinitely pinned WAL. No observer checkpoint ran during a measured window.
After stopping each measured daemon, a separate diagnostic connection ran
`PRAGMA wal_checkpoint(PASSIVE)`:

| Binary | Busy | Log frames | Checkpointed frames |
|---|---:|---:|---:|
| Baseline | 0 | 389 | 389 |
| Previous fix | 0 | 828 | 828 |
| Now | 0 | 29 | 29 |

The production status-route regression also keeps the read-only connection
alive across three requests and writer changes, checking that PASSIVE can
checkpoint every frame between requests. Adding a persistent `BEGIN` to the
reader makes that test fail. Read statements already finish within their
store methods; no transaction survives a completed status request.

SQLite's default 1000-frame autocheckpoint and FULL synchronous mode are
retained. The evidence supports keeping short read transactions and removing
unnecessary updates. A periodic TRUNCATE would reclaim the allocated WAL space
but add checkpoint work without stopping summary churn. No checkpoint timer
or durability trade-off is needed for this fix.

### Validation and retained evidence

Boat `bx_q2mxq7m9` ran 463 workspace tests with zero failures or ignored tests,
plus `cargo clippy --workspace --all-targets -j2 -- -D warnings`. Four deliberate
mutations failed their focused tests: dropping a source's tokens, ignoring
source recency, refreshing undated activity, and pinning the status reader's
transaction. All were restored. Focused tests and Clippy passed after restoration.
The existing socket/TCP authentication and startup writer-lock tests passed.
Boat is stopped.

All three scratch databases passed integrity checking and retained every token
row from the seed. No test suite ran locally. The three main measurement runs
were sequential; the release build ran between the previous and final runs.
The same read-only bubblewrap host, private sockets, port 0, disabled executable
PATH, and network namespace isolated each daemon. The live service remained
PID 3340986. Nothing was pushed or rebuilt through NixOS.

Files under `/home/mini/Documents/repos/usagewindow.research/startup-memory/`:

- `measure_writes.py`: runnable phase-separated measurement harness. It samples
  WAL size and SQLite's WAL-index frame/backfill counters without checkpointing
  the running daemon. SQLite's bundled `WalIndexHdr`/`WalCkptInfo` layout supplies
  the offsets. The post-stop checkpoint is excluded from measured writes.
- `writes-{baseline,previous,now}/`: raw samples, request timings, summaries,
  daemon logs, and retained WAL snapshots. Scratch database copies were removed
  after integrity, preservation, and page-attribution checks.
- `writes-comparison.json` and `writes-comparison.html`: the phase comparison,
  checkpoint/frame data, and chart.
- `writes-binaries.json`: executable hashes, source identities and SQLite defaults.
  The final executable is retained as `uw-daemon-writes`.
- `reproduce_writes.py`, `reproduction.json`, and `repro-*/syscalls.*`: a small
  synthetic same-parent multi-transcript syscall trace, separate from the main
  measurements.
- `writes-boat-evidence.tar.gz`: full-suite, Clippy, focused-test and mutation logs.

Scan offsets remain in memory. Persisting a cursor across restarts would need
the file-generation checks and usage commit to agree on the same checkpoint;
that is separate work and was not needed to remove steady-state write churn.
