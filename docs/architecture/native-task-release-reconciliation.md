# Native task release reconciliation

Canonical task grants are limits, not resident-byte reservations. Native pool growth
charges the shared allocation listener; serialized buffer grants are exact admission
reservations. Task budget release follows actual Native driver, Flight/Arrow reader,
and output-owner drain. A zero handle alone is not drain evidence.

Cancellation propagates reader-revocation and Native cancellation failures. Native
cancellation is still attempted when reader revocation fails; distinct failures are
retained together. Failed cancellation cannot acknowledge a successful cancellation.

Native release returns its final joined TaskReport before the physical owner is
retired. Worker stages that exact return before Java statistics reconciliation. On
retry, Worker validates the full TaskAttemptId, Native report origin, terminal state,
execution drain, and required statistics before using the staged return. It does
not send shuffle-stop or exchange-control commands to an already retired owner.
The existing handle CAS and shared release futures still govern budget release.
An actual receipt belonging to another attempt is rejected before any budget release.
After a speculative loser is explicitly retired, its delayed Worker terminal HTTP
report may carry the same Native-owner-released receipt with a Worker-reconciled
origin. Coordinator acknowledges that report only when the issued attempt, worker,
terminal state, error identity, and drain checkpoint match the retained Native
receipt. The acknowledgement does not republish output or revive the retired task.
Different failures and closed-without-owner receipts remain distinct.

TaskReport has no SecurityContext field. Security provenance remains the admitted
immutable TaskRequest in NativeTaskExecutor's actual creation owner; its final release
checks the exact attempt against that owner before returning a receipt. Staged byte
arrays are internal lifecycle state, not an externally accepted authorization token.
This does not introduce a new security attestation into TaskReport.

The native PANGROVA arbitrator still needs separate matching-cohort qualification for
real operator spill, callback accounting across native threads, owner retirement,
and runtime shutdown. Java cancellation/reconciliation checks do not qualify those
Native changes or establish a globally wired critical-pressure victim selector.
