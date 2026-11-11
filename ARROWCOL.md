# ArrowCol integration branch

This branch (`arrowcol/integration`) is the reference branch that ArrowCol builds against. It carries every otel-arrow change ArrowCol needs, ahead of upstream review. Nothing on this branch is opened as an upstream PR directly, and this file and `.github/workflows/arrowcol-ci.yml` are never upstreamed.

## Branch rules

1. Fork `main` always equals upstream `main`.
2. Each change is committed directly to `arrowcol/integration` as one or more commits, each with an `Upstream-Slice:` trailer that maps it to a row in the table below. ArrowCol pins an `arrowcol-YYYYMMDD` tag of this branch. A tag is created only when ArrowCol is ready to consume a new state of the branch, not for every change. Fork CI runs on every push to this branch.
3. Fork-only commits (this file and the fork CI) use `Upstream-Slice: none`.
4. History on this branch is never rewritten once ArrowCol pins one of its tags. After that, fixes land as new commits with the same `Upstream-Slice:` trailer, and a pinned tag never moves.
5. Each upstream PR gets its own `upstream/<topic>` branch, cut from upstream `main` (or from the previous PR's branch when stacked). It ports only that slice's change. Review follow-ups are made on the PR branch.
6. After an upstream PR merges, sync fork `main` with upstream `main` and merge `main` into this branch. On conflicts, keep upstream's merged version. Run the fork CI, record the merge commit below, and delete the PR branch. Tag only when ArrowCol needs a new pin (rule 2).

Note that commits ported from the prototype fork (`mdrakiburrahman/otel-arrow`) keep their original author and carry a `(cherry picked from commit ...)` line. Port notes in the commit body describe anything that changed during the port.

## Changes

| Slice | Change | Upstream task | Source commit | Integration commit | Upstream PR | Upstream merge |
| --- | --- | --- | --- | --- | --- | --- |
| R1-R3 | Pluggable Arrow pdata representation | Upstream-2 | 4629d611 | 995c9df3 | | |
| D1 | Quiver opaque per-bundle metadata | Upstream-7 | 04e8bd00 (part) | 0ab4d9cd | | |
| W1 | `durable_buffer` WAL flush interval | Upstream-5 | 04e8bd00 (part) | feffdb26 | | |
| D2 | `durable_buffer` support for Arrow representations | Upstream-3 | 04e8bd00 (part) | 77fad053 | | |
| Q1 | Quiver WAL reopen after rotated files are purged, and after a crash during rotation (#4682) | Upstream-1 | f98395e6, JakeDern@60157e83 | 4c8865a9, 262313e5, 177175c6 | | |
| Q2 | Serialized Quiver segment finalization | Upstream-1 | 64a61451 | 0d012de2 | | |
| Q4 | Read-only Quiver WAL inspection API | none yet | 542b1551 | 13206878 | | |
| E2 | Embedding instrumentation hooks in telemetry | Upstream-12 | c5c8cb3e (part) | 74cf5fa6 | | |
| Q3 | Delete Quiver segments only after completion is durable | Upstream-1 | 395fcf16 | c74ceef5 | | |
| Q6 | Crash-durable Quiver file replacement on Windows (#4139) | Upstream-1 | none (new) | 26307c5f | | |
| T1 | Key-routed balanced topic delivery | Upstream-4 | none (new) | ee09f44a | | |
| W2 | Quiver WAL and segment compression | Upstream-5 | none (new) | a4d564c0 | | |
| W3 | Upstream ACK after WAL sync | Upstream-5 | none (new) | 891759cb | | |
| F1 | Forward after ingest | Upstream-6 | none (new) | d75aa6c1 | | |
| G1 | Quiver data directory ownership (#3455) | Upstream-8 | none (new) | 1b13fb87 | | |
| G2 | Quiver fencing of a previous data directory owner (#3455) | Upstream-8 | none (new) | 80f24f4a | | |
| G3 | `durable_buffer` storage directory handoff during a live `replace` | Upstream-8 | none (new) | e362fa1d | | |

Q4 stays on this branch until an upstream consumer exists.
