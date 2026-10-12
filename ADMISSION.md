# Native admission extension

This fork starts from `fed8e9eb35f9cd3e5c2a67842c924346c3e1fbdd` and retains the original MIT license. The v2 extension adds:

- `TwoStepCircuitBreaker.AllowWithState() (State, func(error), error)`: the returned state is the state actually evaluated under the native admission mutex. Open and occupied half-open refusals return that state and a nil completion. The completion retains the admitted generation and rolling-window age, invokes the native completion path once, and cannot charge a newer generation or expired bucket. Existing `Allow` uses this same path; `Execute` uses the same native admission implementation. There is no separate `State()` lookup or second breaker.
- `Settings.Now func() time.Time`: nil defaults to `time.Now`. The clock is fixed on construction and supplies constructor, state, admission and completion time. A custom clock must be concurrency-safe, nonblocking and non-reentrant; it should advance monotonically. The hook does not replace the distributed breaker's remote-lock clock or qualify distributed admission.

The deterministic tests exercise closed/open/half-open refusal and recovery, stale generation/window completion, concurrent half-open admission and duplicate completion. They advance a private atomic clock without real sleep or mirrored breaker state. CI also runs the original upstream module suites and a race-enabled extension job.

This fork supplies native admission and test clock primitives. Product request classification, safe probe selection, fleet policy and deployment qualification remain with the product caller.
