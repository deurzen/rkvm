# rkvm local task board

Priority order is based on expected impact on input latency/perceived speed first, then robustness, then implementation risk. Tasks are removed before the commit that completes them.

## P4 — Drain ready evdev events by frame

Reduce per-event async readiness overhead on input capture.

Acceptance:
- After readiness, drain available libevdev events until frame completion or would-block.
- Preserve `SYN_DROPPED` handling and local echo for unrecognized events.
- Preserve cancel safety for partially written local echo events.

## P5 — Precompute switch-key membership

Avoid scanning every configured switch binding for every key event.

Acceptance:
- Build a switch-key union set once from configured bindings.
- Use the precomputed set in the hot routing path.
- Preserve chord matching and active-binding behavior.
- Keep existing switch-binding tests passing.
