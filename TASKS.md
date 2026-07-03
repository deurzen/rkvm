# rkvm local task board

Priority order is based on expected impact on input latency/perceived speed first, then robustness, then implementation risk. Tasks are removed before the commit that completes them.

## P2 — Make heartbeat traffic idle-only

Prevent periodic pings from preempting input updates or blocking the per-client writer while waiting for a pong.

Acceptance:
- Treat any valid update received by the client as server liveness.
- Server sends pings only after an idle period without queued updates.
- Waiting for pong must not add avoidable latency to queued input updates.
- Preserve disconnect detection for idle broken connections.

## P3 — Remove hot-path tracing overhead from input reads

Avoid per-event span construction and dynamic path lookup in `Interceptor::read`.

Acceptance:
- Remove or reduce per-event `tracing::instrument` overhead.
- Keep useful registration/error logging intact.
- Preserve input read behavior exactly.

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
