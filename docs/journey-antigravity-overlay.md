# Antigravity on the deployed overlay branch

The earlier pilot, `neon/antigravity` at `2901182`, started from `4f219d1`.
This port starts from deployed overlay branch `ff56157` and reuses its per-session
cache TTL fields and scheduling machinery.

The port retains the pilot's quota parsing, conversation token accounting,
statev2 discovery, and equal-timestamp sibling-window merge. T3Code conflict
resolution keeps the overlay branch's ownership and lineage routing, reason-aware
compaction instructions, and external-status transport. Gemini compact and resume
refusals run before ownership lookup or delivery. The v2 pilot lookup initially
skipped parent ownership. A failing child-session regression exposed that mismatch;
both database formats now feed the existing direct-first lineage candidate order.

MCP's production dependencies now proxy the daemon. The old pilot's local adapter
registration was therefore dropped; sibling-window regression coverage remains.
Daemon registration uses the newer native-adapter registry.

The unknown-TTL policy helper from `f6f4511` was not ported. The probe log showed
8,334 cached tokens after a 24-minute gap at 2026-11-21T06:00:21Z. With the requested
one-minute margin, discovery supplies a 23-minute per-session TTL. A discovery
test failed on `None` versus 1,380 seconds before that assignment was added.
A vertical-path test checks persistence and the warm/cold boundary.

Reviewing production wiring exposed a further mismatch: compaction fallback
wrapping promotes native capabilities. A regression test failed when that wrapper
made Gemini actionable. Gemini now bypasses that wrapper; other providers retain
existing fallback behavior. The hard-boundary capability gate remains necessary
before interrupting or enqueueing compaction.

Verification runs on Boat `bx_f5vb4mfu`; local cargo and Nix builds are excluded.
The handoff report in `mstack.research/neon-trial/report-uw-port.md` records check
results, mutations, and remaining uncertainty. No UI or mock-up changes apply.
