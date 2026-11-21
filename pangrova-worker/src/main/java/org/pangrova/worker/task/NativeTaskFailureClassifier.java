package org.pangrova.worker.task;

import org.pangrova.worker.enums.NativeTaskFailureCategory;
import org.pangrova.worker.exception.NativeRuntimeException;

import java.util.Locale;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Classifies exceptions thrown by NativeTaskExecutor.isFinished() into
 * actionable categories for the completion monitor.
 *
 * <p>Classification logic uses cause chain matching:</p>
 * <ul>
 *   <li>NATIVE_CRASHED — C++ SIGSEGV/abort signals; requires fail-fast</li>
 *   <li>NATIVE_HUNG — no response within heartbeat window; escalate as timeout</li>
 *   <li>NATIVE_TASK_FAILED — definitive task failure reported by native engine; fail-fast</li>
 *   <li>TRANSIENT — short-lived IO jitter; backoff retry up to max attempts</li>
 * </ul>
 */
public final class NativeTaskFailureClassifier {

    private NativeTaskFailureClassifier() {}

    /**
     * Classifies the given throwable by inspecting the exception type and cause chain.
     *
     * @param t the throwable from isFinished()
     * @return the failure category determining the response strategy
     */
    public static NativeTaskFailureCategory classify(Throwable t) {
        if (isCrashSignal(t)) {
            return NativeTaskFailureCategory.NATIVE_CRASHED;
        }
        if (isHungSignal(t)) {
            return NativeTaskFailureCategory.NATIVE_HUNG;
        }
        if (isDefinitiveTaskFailure(t)) {
            return NativeTaskFailureCategory.NATIVE_TASK_FAILED;
        }
        Throwable cause = t.getCause();
        while (cause != null) {
            if (isCrashSignal(cause)) {
                return NativeTaskFailureCategory.NATIVE_CRASHED;
            }
            if (isHungSignal(cause)) {
                return NativeTaskFailureCategory.NATIVE_HUNG;
            }
            if (isDefinitiveTaskFailure(cause)) {
                return NativeTaskFailureCategory.NATIVE_TASK_FAILED;
            }
            cause = cause.getCause();
        }
        return NativeTaskFailureCategory.TRANSIENT;
    }

    /** Lower-case message patterns indicating a native process crash. */
    private static final List<String> CRASH_MESSAGE_PATTERNS = List.of(
            "sigsegv", "signal 11", "sigabrt", "signal 6",
            "segmentation fault", "native crash");

    private static boolean isCrashSignal(Throwable t) {
        String className = t.getClass().getSimpleName();
        if ("SigsegvException".equals(className) || "AbortException".equals(className)) {
            return true;
        }
        String message = t.getMessage();
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        for (String pattern : CRASH_MESSAGE_PATTERNS) {
            if (lower.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDefinitiveTaskFailure(Throwable t) {
        return t instanceof NativeRuntimeException;
    }

    private static boolean isHungSignal(Throwable t) {
        return t instanceof TimeoutException
                || "DeadlineExceededException".equals(t.getClass().getSimpleName());
    }
}
