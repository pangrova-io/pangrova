package org.pangrova.operations.diagnostics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Writes heap dumps using the HotSpot Diagnostic MXBean.
 */
final class HeapDumpWriter {

    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

    /**
     * Writes a heap dump to the specified path.
     *
     * @param outputPath the file path for the heap dump
     * @param liveOnly   if true, only dump live objects (triggers GC first)
     */
    void write(Path outputPath, boolean liveOnly) throws IOException {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(HOTSPOT_BEAN_NAME);
            server.invoke(objectName, "dumpHeap",
                    new Object[]{outputPath.toAbsolutePath().toString(), liveOnly},
                    new String[]{String.class.getName(), boolean.class.getName()});
        } catch (Exception e) {
            if (e.getCause() instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException("Failed to invoke HotSpot dumpHeap MBean", e);
        }
    }
}
