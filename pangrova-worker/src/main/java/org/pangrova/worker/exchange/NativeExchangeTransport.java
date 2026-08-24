package org.pangrova.worker.exchange;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.jni.NativeCommandType;
import org.pangrova.worker.jni.proto.ConnectExchangeSourcePayload;
import org.pangrova.worker.jni.proto.ExchangeSourceInfo;
import org.pangrova.worker.jni.proto.StartNativeExchangePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Native data exchange transport delegating to C++ via JNI.
 *
 * <p>Exchange data flows entirely within C++: upstream FlightShuffleServer →
 * data exchange → downstream PangrovaExchangeSource (active mode) → native
 * Exchange operator. Java only issues the initial connection command.</p>
 *
 * <p>JNI command used:</p>
 * <ul>
 *   <li>{@code START_NATIVE_EXCHANGE} — instructs C++ to create
 *       PangrovaExchangeSource instances in active Flight mode for each
 *       upstream source</li>
 * </ul>
 *
 * @see ExchangeTransport
 */
public final class NativeExchangeTransport implements ExchangeTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NativeExchangeTransport.class);

    private final TaskExecutor taskExecutor;

    /**
     * @param taskExecutor native task executor for issuing JNI commands
     */
    public NativeExchangeTransport(TaskExecutor taskExecutor) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
    }

    /**
     * Starts native exchange by instructing C++ to create PangrovaExchangeSource
     * instances in active Flight mode for each upstream source.
     *
     * <p>The native side creates one PangrovaExchangeSource per source, calls
     * setFlightEndpoint() on each, and registers them with the native
     * ExchangeNode. Data flows directly between C++ workers via data exchange,
     * bypassing Java entirely.</p>
     */
    @Override
    public void startExchange(long executorHandle, List<ExchangeEndpoint> sources) {
        if (executorHandle == 0) {
            throw new IllegalArgumentException("executorHandle must not be zero");
        }
        checkNotNull(sources, "sources must not be null");

        byte[] commandPayload = buildStartNativeExchangePayload(sources);
        byte[] command = NativeCommandBuilder.buildCommand(executorHandle, NativeCommandType.START_NATIVE_EXCHANGE,
                commandPayload);
        taskExecutor.executeCommand(executorHandle, command);

        LOG.info("event=native_exchange_started executorHandle={} sourceCount={}",
                executorHandle, sources.size());
    }

    /**
     * Dynamically adds new exchange sources via JNI.
     *
     * <p>Each new source triggers a CONNECT_EXCHANGE_SOURCE command to the
     * native side, which creates an additional PangrovaExchangeSource in
     * active Flight mode.</p>
     */
    @Override
    public void addSources(long executorHandle, List<ExchangeEndpoint> newSources) {
        checkNotNull(newSources, "newSources must not be null");
        for (ExchangeEndpoint source : newSources) {
            byte[] commandPayload = buildConnectSourcePayload(source);
            byte[] command = NativeCommandBuilder.buildCommand(executorHandle,
                    NativeCommandType.CONNECT_EXCHANGE_SOURCE, commandPayload);
            taskExecutor.executeCommand(executorHandle, command);
        }
        LOG.debug("event=native_exchange_sources_added executorHandle={} count={}",
                executorHandle, newSources.size());
    }

    @Override
    public void noMoreSources(long executorHandle) {
        if (executorHandle == 0) {
            LOG.debug("event=native_exchange_no_more_sources_skipped reason=zero_handle");
            return;
        }
        byte[] command = NativeCommandBuilder.buildCommand(
                executorHandle, NativeCommandType.NO_MORE_EXCHANGE_SOURCES, new byte[0]);
        taskExecutor.executeCommand(executorHandle, command);
        LOG.debug("event=native_exchange_no_more_sources executorHandle={}", executorHandle);
    }

    // --- Proto-generated payload encoding ---

    /**
     * Builds StartNativeExchangeCommand payload using generated proto.
     */
    private static byte[] buildStartNativeExchangePayload(List<ExchangeEndpoint> sources) {
        StartNativeExchangePayload.Builder builder = StartNativeExchangePayload.newBuilder();
        for (ExchangeEndpoint source : sources) {
            builder.addSources(ExchangeSourceInfo.newBuilder()
                    .setSourceTaskId(source.sourceTaskId())
                    .setHost(source.host())
                    .setPort(source.transportPort())
                    .setPartitionId(source.partitionId())
                    .build());
        }
        return builder.build().toByteArray();
    }

    private static byte[] buildConnectSourcePayload(ExchangeEndpoint source) {
        return ConnectExchangeSourcePayload.newBuilder()
                .setSourceId(source.sourceTaskId())
                .setHost(source.host())
                .setPort(source.transportPort())
                .setPartitionId(source.partitionId())
                .build()
                .toByteArray();
    }
}
