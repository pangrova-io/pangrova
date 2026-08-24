package org.pangrova.shuffle;

import java.nio.ByteBuffer;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI for the native-to-JVM byte egress data path (ADR-002 D2-egress).
 *
 * <p>Today the native shuffle path is <em>purely control-channel</em>: {@code NativeShuffleTransport}
 * sends START/CONNECT/STOP commands via {@code executeCommand}, and all shuffle bytes stay entirely
 * in C++ (native engine -> SerializedShuffleQueue -> FlightShuffleServer -> Arrow Flight). For the
 * JVM-resident providers ({@code local-spill}, {@code object-store}, {@code celeborn}), committed
 * partition bytes must cross from native to JVM so they can be written to {@code SpoolStore} or
 * pushed to an external RSS.
 *
 * <p>This interface defines the contract for that data path. Implementations will use either:
 * <ul>
 *   <li>A new {@code NativeCommandType} that carries Arrow IPC bytes from the native
 *       {@code SerializedShuffleQueue} across JNI, or</li>
 *   <li>Arrow C Data Interface pointers passed via JNI for zero-copy transfer.</li>
 * </ul>
 *
 * <p>The {@code native} provider does <em>not</em> use this path (bytes never leave C++). Only the
 * three JVM-resident providers share this egress SPI.
 *
 * <p><strong>Stub status:</strong> this interface defines the contract but has no implementation yet.
 * The JNI data path must be built in {@code pangrova-worker-native} (C++ side) and
 * {@code pangrova-worker} (Java side) before the JVM-resident providers can function.
 */
@Experimental(reason = "ADR-002 D2-egress: native->JVM byte path for JVM-resident shuffle providers")
public interface ShuffleByteEgress {

    /**
     * Called when the native layer has committed a partition segment that must be delivered to a
     * JVM-resident shuffle provider.
     *
     * @param queryId     the query identifier
     * @param stageId     the producing stage
     * @param partitionId the target reduce partition
     * @param mapId       the producer (mapper) index within the stage
     * @param attemptId   the producer task attempt
     * @param arrowIpc    the Arrow IPC encoded bytes of the committed segment
     */
    void onPartitionCommitted(String queryId, int stageId, int partitionId,
                              int mapId, long attemptId, ByteBuffer arrowIpc);
}
