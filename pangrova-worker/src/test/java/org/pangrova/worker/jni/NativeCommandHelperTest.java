package org.pangrova.worker.jni;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.PrefetchArtifactsRequest;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.protocol.v2.UpdateResourceGrantRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract tests for the canonical v2 JVM-to-Native command envelope. */
class NativeCommandBuilderTest {

    @Test
    void updateResourceGrantIsTypedAndDoesNotDuplicateProcessHandle() throws Exception {
        RuntimeCommand command = parseCommand(NativeCommandBuilder.updateMemoryLimit(1024 * 1024L));
        assertThat(command.getProtocol().getMajor()).isEqualTo(2);
        assertThat(command.getCommandType())
                .isEqualTo(RuntimeCommandType.RUNTIME_COMMAND_TYPE_UPDATE_RESOURCE_GRANT);
        UpdateResourceGrantRequest request = command.getPayload()
                .unpack(UpdateResourceGrantRequest.class);
        assertThat(request.getResourceGrant().getNativeMemoryBytes()).isEqualTo(1024 * 1024L);
        assertThat(command.getAttempt().getTaskId()).isEmpty();
    }

    @Test
    void commandTypesProduceCanonicalEnvelopes() {
        assertThat(parseCommand(NativeCommandBuilder.getMetrics()).getCommandType())
                .isEqualTo(RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_METRICS);
        assertThat(parseCommand(NativeCommandBuilder.buildGetCapabilities()).getCommandType())
                .isEqualTo(RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_CAPABILITIES);
        assertThat(parseCommand(NativeCommandBuilder.evictCache("s3://bucket/path/*")).getPayload())
                .isNotEqualTo(Any.getDefaultInstance());
        assertThat(parseCommand(NativeCommandBuilder.prefetchArtifacts(
                PrefetchArtifactsRequest.getDefaultInstance())).getCommandType())
                .isEqualTo(RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREFETCH_ARTIFACTS);
    }

    @Test
    void runtimeFilterUsesTypedV2Payload() throws Exception {
        InjectRuntimeFilterRequest filterRequest = InjectRuntimeFilterRequest.newBuilder()
                .setPlanNodeId("node_0")
                .addFilters(RuntimeFilter.newBuilder()
                        .setColumnName("order_id")
                        .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER)
                        .setRange(RangeFilter.newBuilder()
                                .setLower(TypedValue.newBuilder().setInt64Value(1))
                                .setUpper(TypedValue.newBuilder().setInt64Value(100))
                                .setLowerInclusive(true)
                                .setUpperInclusive(true)))
                .build();
        RuntimeCommand command = parseCommand(NativeCommandBuilder.injectRuntimeFilter(filterRequest));
        assertThat(command.getCommandType())
                .isEqualTo(RuntimeCommandType.RUNTIME_COMMAND_TYPE_INJECT_RUNTIME_FILTER);
        assertThat(command.getPayload().unpack(InjectRuntimeFilterRequest.class))
                .isEqualTo(filterRequest);
    }

    @Test
    void runtimeFilterRequiresAScanAndTypedPredicates() {
        assertThatThrownBy(() -> NativeCommandBuilder.injectRuntimeFilter(
                InjectRuntimeFilterRequest.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeCommandBuilder.injectRuntimeFilter(
                InjectRuntimeFilterRequest.newBuilder().setPlanNodeId("node_0").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scan node and filters");
    }

    @Test
    void responseHandlingFailsClosedAndExtractsAnyValue() {
        assertThatThrownBy(() -> NativeCommandBuilder.extractResultPayload(null))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> NativeCommandBuilder.extractResultPayload(new byte[0]))
                .isInstanceOf(IllegalStateException.class);

        byte[] failure = RuntimeCommandResponse.newBuilder()
                .setSuccess(false)
                .setError(ExecutionError.newBuilder()
                        .setErrorClass(ErrorClass.ERROR_CLASS_INVALID_REQUEST)
                        .setMessage("rejected"))
                .build()
                .toByteArray();
        assertThatThrownBy(() -> NativeCommandBuilder.extractResultPayload(failure))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rejected");

        byte[] response = RuntimeCommandResponse.newBuilder()
                .setSuccess(true)
                .setPayload(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/test.Raw")
                        .setValue(ByteString.copyFromUtf8("ok")))
                .build()
                .toByteArray();
        assertThat(NativeCommandBuilder.extractResultPayload(response))
                .isEqualTo("ok".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void invalidArgumentsAreRejected() {
        assertThatThrownBy(() -> NativeCommandBuilder.updateMemoryLimit(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeCommandBuilder.evictCache(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_UNSPECIFIED, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RuntimeCommand parseCommand(byte[] bytes) {
        try {
            return RuntimeCommand.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError(failure);
        }
    }
}
