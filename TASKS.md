# rkvm local task board

Priority order is based on expected impact on input latency/perceived speed first, then robustness, then implementation risk. Tasks are removed before the commit that completes them.

## P5 — Precompute switch-key membership

Avoid scanning every configured switch binding for every key event.

Acceptance:
- Build a switch-key union set once from configured bindings.
- Use the precomputed set in the hot routing path.
- Preserve chord matching and active-binding behavior.
- Keep existing switch-binding tests passing.
