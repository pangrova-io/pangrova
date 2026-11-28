package org.pangrova.worker.heartbeat;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

/**
 * Reads process CPU utilization via the JMX {@code OperatingSystemMXBean}.
 *
 * <p>Uses {@code com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()}
 * which returns a value in [0.0, 1.0] representing the fraction of CPU time
 * used by the JVM process. Falls back to 0.0 if the platform MXBean is
 * unavailable (e.g., non-HotSpot JVM).</p>
 *
 * <p>Thread-safe: the underlying MXBean is safe to call from any thread.</p>
 */
@ThreadSafe
public final class CpuUsageReader {

    private static final Logger LOG = LoggerFactory.getLogger(CpuUsageReader.class);

    private static final com.sun.management.OperatingSystemMXBean OS_BEAN;

    static {
        com.sun.management.OperatingSystemMXBean bean = null;
        try {
            bean = (com.sun.management.OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean();
        } catch (ClassCastException | LinkageError e) {
            LOG.warn("event=cpu_usage_reader_init_failed reason={} — cpu_utilization will report 0.0",
                    e.toString());
        }
        OS_BEAN = bean;
    }

    private CpuUsageReader() {}

    /**
     * Returns the process CPU load as a value in [0.0, 1.0].
     *
     * <p>A negative value from the JMX bean (reported when the value is not
     * yet available, typically immediately after JVM start) is clamped to 0.0.</p>
     *
     * @return CPU utilization fraction, or 0.0 if unavailable
     */
    public static double getProcessCpuLoad() {
        if (OS_BEAN == null) {
            return 0.0;
        }
        try {
            double load = OS_BEAN.getProcessCpuLoad();
            // JMX may return negative value if not yet computed
            return load < 0.0 ? 0.0 : load;
        } catch (RuntimeException e) {
            LOG.debug("event=cpu_usage_read_failed error={}", e.getMessage());
            return 0.0;
        }
    }
}
