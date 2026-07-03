# rkvm local task board

Priority order is based on expected impact on input latency/perceived speed first, then robustness, then implementation risk. Tasks are removed before the commit that completes them.

## P0 — Reuse network encode buffers

Reduce allocator churn in the hot outbound message path.

Acceptance:
- Add a reusable-buffer encode path mirroring `decode_with_buffer`.
- Keep the existing length-prefixed wire format unchanged.
- Use the reusable encode path in latency-sensitive client/server loops.
- Add tests for wire compatibility and buffer capacity reuse.

## P1 — Write received input frames to uinput in batches

Avoid per-event async readiness overhead when replaying a received `Update::Events` frame.

Acceptance:
- Add a `Writer` frame/batch write API that preserves event order.
- Handle partial writes/would-block without duplicating or reordering events.
- Use the batch API on the client for `Update::Events`.
- Keep single-event writes available for existing call sites.

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
