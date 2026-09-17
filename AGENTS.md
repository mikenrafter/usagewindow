# AGENTS.md — usagewindow

Harness-agnostic usage-window and resume manager for coding-agent harnesses (Claude
Code, Codex, others). Rust library (`uw-core`) first; CLI (`uw`) and web UI are thin
consumers. See `docs/architecture.md` for the full design and `docs/research-*.md` for
harness-specific integration research — read both before writing adapter or policy code.

## Working conventions

- **TDD, strictly.** For each unit of work: write a failing test first, then implement
  until it passes. `cargo test` and `cargo clippy --all-targets -- -D warnings` must be
  green before you consider a phase done.
- **Pure logic stays pure.** `uw-policy` must have zero I/O — every function there takes
  plain data in, returns plain data out, so it's testable with synthetic sample
  sequences without a live harness or a database.
- **`uw-core` has zero harness-specific code.** Anything Claude-Code-specific or
  Codex-specific belongs in `uw-adapters`, behind the `HarnessAdapter` trait.
- **Never guess at a harness's hook/API behavior.** If `docs/research-<harness>.md`
  doesn't cover something you need, that's a research gap — write it up in that file
  (with what you tested/verified) before writing adapter code against an assumption.
- **Compaction/resume delivery is destructive and non-idempotent.** Any code path that
  sends a message to a live agent session or respawns a harness process must go through
  the claim-before-act queue pattern described in `docs/architecture.md` — claim first
  (atomic status transition), fail closed on any error, never blind-retry.
- **Ask-only vs auto-act is a deliberate, per-policy distinction** — see
  `docs/architecture.md`'s trigger-policy section for which policies may act
  unilaterally (opportunistic idle-compact, resume scheduling) and which must only ask
  the agent and let it decide (near-limit compaction). Don't blur this line when adding
  a new policy.
- Commit with clear, scoped messages per logical change — don't bundle unrelated crates'
  changes into one commit.

## Repo layout

See `docs/architecture.md` for the crate-by-crate breakdown and the 9-phase build order.
