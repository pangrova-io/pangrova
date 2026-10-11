# Exact cancellation admission and retryable physical drain

## Actual failures and authorities

The genuine packaged-JNI cancellation run failed at the exact admission registry guard. The Native request and release receipt had the same attempt; `ActualTaskManagerFixture.preparePending` had inserted a TaskInfo directly without `TaskAdmissionRegistry.reserve/publish/admitted`. The missing formal entry made `retainDrainReceipt` correctly reject the release. The fixture now reserves the same canonical request (including deadline/checksum), runs the existing control-epoch and execution-owner checks, publishes through the reservation and commits admission before actual Native creation. The actual created-but-not-started boundary remains available, and no admission/receipt guard is relaxed.

The real stalled Flight RPC case then exposed a production cleanup gap. `NativeTaskExecutor.releaseTask` seals `NativeOperationLifetime` before joining; a real Native refusal (`Result RPC readers or exported buffers still own task resources`) must retain the owner and grant. The old permanent seal also rejected the actual GET_RESOURCE_DRAIN_EVIDENCE and STOP_SHUFFLE_SERVER commands needed to observe/retry drain. Removing the seal would permit new execution and is not the fix.

## Single-owner cleanup protocol

Only the existing two typed commands GET_RESOURCE_DRAIN_EVIDENCE and STOP_SHUFFLE_SERVER receive a cleanup lease. Their original Native request parsing, exact task/security validation and command dispatch remain intact. Normal start, status/read/other command operations still use the original sealed admission and remain rejected. No configuration switch, alternative executor or simulated report is added.

Cleanup uses the same owner readers/exit future as ordinary admitted JNI operations. An active exclusive release makes cleanup wait for its actual exit; the wait occurs outside the executor lifecycle lock. A retrying exclusive release waits for an admitted cleanup operation to leave. Closing/removing an owner or obtaining its actual Native release receipt rejects further physical command admission. A failed Native release retains its real owner and the existing production continuation, never converting a refused join to successful exit. Self-join remains rejected.

## Oracle preservation and validation boundaries

All original assertions in the eight TaskCancellationResourceCleanupTest cases and five NativeOperationLifetimeTest cases remain unchanged. The created-but-not-started positive cases now have genuine exact admission records. The stalled-RPC case still holds its live actual Flight stream, requires real producer stop, requires Native release failure, verifies the same handle/buffer grant remains and forbids terminal cancellation publication until the physical owner leaves. It additionally verifies ordinary start remains sealed and actual drain evidence remains false after the failed release. Closing the actual stream precedes the same original production cleanup sweep, exact release receipt and budget assertions.

A sixth actual-Runnable lifetime test verifies failed exclusive cleanup, no reopened execution, a leased cleanup observation blocking the real retry thread, and release only after its actual final exit. These are Java owner/barrier tests, not Native backend evidence.

The current draft's five Java files compile using JDK17 against the owning module's resolved actual classpath. NativeOperationLifetimeTest runs 6/6 with zero skipped tests. The eight genuine packaged-JNI cancellation cases are pending the root-controlled rerun; the earlier Output9/9 and Exchange5/5 results are not attributed to this unexecuted cancellation unit. The existing descriptor/collecting observer limitations remain explicit; no SQL/distributed E2E or remote serving-owner drain claim is made.
