# Startup and memory investigation, 2026-11-19

The socket now serves stored data while discovery warms in the background. The
15-minute measurements compare deployed commit `791b690` with the local release
build on `fix/startup-and-memory`. These are isolated measurements on nixed, not
measurements of a restarted system service.

| Metric | Deployed 791b690 | Profiled local release |
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

Physical writes increased 3.85 times in this run.
That increase is unresolved; the writer and checkpoint paths need further
profiling before treating this as an overall I/O improvement. RSS grew another
67.0 MiB after minute three, compared with
199.9 MiB in the baseline. This experiment establishes
a lower 15-minute peak, not a proven long-run plateau or a reproduced/fixed
2 GiB service-cgroup peak.

Local implementation commits, not pushed:

- `f8cc40f` Serve stored status promptly during daemon warmup
- `1dc4d1b` Scan transcript appends off the runtime and acknowledge committed usage
- `05f694e` Reuse account window analysis within each policy pass
- `e01c94a` Guard incremental scans against changed owners and delayed newlines
