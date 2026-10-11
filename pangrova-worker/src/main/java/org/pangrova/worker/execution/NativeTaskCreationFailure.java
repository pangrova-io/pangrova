package org.pangrova.worker.execution;

/** Carries a known actual Native owner through a failed Java publication; never invents an absent owner. */
public final class NativeTaskCreationFailure extends IllegalStateException {
    private final long actualHandle;
    private final boolean withoutNativeOwner;

    NativeTaskCreationFailure(long actualHandle, Throwable failure) {
        this(actualHandle, false, failure);
    }

    NativeTaskCreationFailure(long actualHandle, boolean withoutNativeOwner, Throwable failure) {
        this(actualHandle, withoutNativeOwner,
                "Native creation did not publish its Java ownership completely", failure);
    }

    private NativeTaskCreationFailure(long actualHandle, boolean withoutNativeOwner,
                                      String message, Throwable failure) {
        super(message, failure);
        this.actualHandle = actualHandle;
        this.withoutNativeOwner = withoutNativeOwner;
    }

    /** Rejected under the lifecycle lock before any new owner or JNI operation. */
    static NativeTaskCreationFailure closedExecutor(boolean hasExistingAttemptOwner) {
        return new NativeTaskCreationFailure(0, !hasExistingAttemptOwner,
                "NativeTaskExecutor is closed", null);
    }

    /** Actual registered Native handle, or zero only when the failure has not supplied one. */
    public long actualHandle() { return actualHandle; }
    /** True only after a verified pre-entry rollback or exact Native no-owner recovery and callback cleanup. */
    public boolean withoutNativeOwner() { return withoutNativeOwner; }
}
