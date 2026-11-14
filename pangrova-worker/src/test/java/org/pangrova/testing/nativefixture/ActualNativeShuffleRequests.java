package org.pangrova.testing.nativefixture;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

/** Actual task-scoped producer registration requests for Native transport tests. */
public final class ActualNativeShuffleRequests {
    private ActualNativeShuffleRequests() {}

    public static TaskRequest pendingProducer(int partitions) {
        var base = ActualNativeTaskFixture.valuesTaskRequest(false);
        var request = base.toBuilder().setTaskKind(TaskKind.TASK_KIND_SHUFFLE_WRITE)
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setShuffle(
                        ShuffleTaskOptions.newBuilder().setSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH)
                                .setCompressionCodec("none").setBatchSize(16)
                                .setPartitionCount(partitions).addPartitionKeyIndices(0)
                                .setPersistenceMode(ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_ONLY)))
                .build();
        CanonicalTaskRequests.validateForAdmission(request);
        return request;
    }

    /** Actual ExchangeRel producer; a pending registration alone omits the sink relation. */
    public static TaskRequest producer(int partitions, byte[] inputPlan) throws Exception {
        var input = io.substrait.proto.Plan.parseFrom(inputPlan);
        if (input.getRelationsCount() != 1 || !input.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException("Producer requires one actual relation root");
        }
        var root = input.getRelations(0).getRoot();
        var exchange = io.substrait.proto.ExchangeRel.newBuilder()
                .setInput(root.getInput()).setPartitionCount(partitions);
        var key = exchange.getScatterByFieldsBuilder().addFieldsBuilder();
        key.getRootReferenceBuilder();
        key.getDirectReferenceBuilder().getStructFieldBuilder().setField(0);
        var plan = input.toBuilder().setRelations(0, input.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(io.substrait.proto.Rel.newBuilder().setExchange(exchange))))
                .build();
        var bytes = plan.toByteArray();
        var request = pendingProducer(partitions).toBuilder()
                .setSubstraitPlan(com.google.protobuf.ByteString.copyFrom(bytes))
                .setSubstraitPlanChecksum(ProtocolChecksums.sha256(bytes)).build();
        CanonicalTaskRequests.validateForAdmission(request);
        return request;
    }
}
