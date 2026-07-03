# rkvm local task board

Priority order is based on expected impact on input latency/perceived speed first, then robustness, then implementation risk.

## P0 — Protocol/event-frame batching

Status: done

Send input frames as batches instead of one network message per individual event.

Acceptance:
- Add a protocol update such as `Update::Events { id, events: Vec<Event> }` or equivalent.
- Server forwards all events belonging to one `SYN_REPORT` frame in one message/flush.
- Client writes each event in order and preserves sync semantics.
- Backward compatibility/version handling is considered, or protocol version is intentionally bumped.
- Validate keyboard and mouse behavior, including key press/release ordering and relative motion.

Notes:
- Likely biggest throughput and latency win.
- Needs careful handling around switch-key propagation and local echo path.

## P1 — TCP_NODELAY for latency-sensitive sockets

Status: done

Disable Nagle on accepted/connected TCP sockets before TLS wrapping.

Acceptance:
- Server calls `set_nodelay(true)` on accepted `TcpStream`s.
- Client calls `set_nodelay(true)` after connect.
- Errors are propagated/logged consistently with existing network errors.
- Basic client/server connection tests still pass.

Notes:
- Small, low-risk patch.
- Should reduce tiny-packet latency and delayed-ACK interactions.

## P2 — Slow-client isolation / bounded backpressure policy

Status: done

Prevent one slow or stuck client from blocking server input processing for everyone.

Acceptance:
- Server event loop cannot await indefinitely on a full per-client channel.
- Slow clients are either dropped after timeout or disconnected on queue overflow.
- Policy is logged clearly.
- Current active-client switching remains correct after disconnect/removal.

Notes:
- Robustness improvement; protects perceived speed under bad network/client conditions.
- Review slab index handling carefully when removing clients.

## P3 — Built-in client reconnect/backoff

Status: done

Let `rkvm-client` reconnect without relying solely on systemd restart.

Acceptance:
- Client retries connection with bounded/exponential backoff after transient disconnects.
- Authentication/TLS/version failures remain visible and do not spin aggressively.
- Existing systemd restart behavior remains acceptable.
- Logs distinguish initial connect, reconnect attempts, and permanent failures.

Notes:
- Improves non-systemd usage and perceived reliability.

## P4 — Reduce per-message allocation in network decode path

Status: todo

Reduce allocator churn in `rkvm-net::message::Message::decode`.

Acceptance:
- Explore reusable buffers or a framed codec without complicating API excessively.
- No change to wire format unless paired with an intentional protocol bump.
- Bench or reason about allocation reduction after event batching is in place.

Notes:
- Lower priority because batching should reduce message count first.

## P5 — Whitelist usability/robustness follow-up

Status: todo

Make the device whitelist easier and safer to use for keyboard-only forwarding.

Acceptance:
- Document recommended path-based matching using `/dev/input/by-id/*-event-kbd` or `/dev/input/by-path/*-event-kbd`.
- Consider a diagnostic mode/log that prints candidate devices with path/name/vendor/product/version.
- Warn users that vendor/product-only can match multiple event nodes from one physical device/receiver.

Notes:
- Complements the whitelist patch; mostly docs/UX unless a list-devices command is added.
