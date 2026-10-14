package org.pangrova.testing.e2e.shuffle;

import com.google.protobuf.Any;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.UUID;

/** Official Arrow producer and generated Substrait consumer, not an execution substitute. */
final class ActualExternalShuffleTask {
    static final String PLAN_NODE = "actual-external-input";
    static final String EXCHANGE_ID = "actual-external-exchange";
    private ActualExternalShuffleTask() {}

    static TaskRequest consumer(ActualShuffleService.AdmittedProducer producer) {
        var schema = NamedStruct.newBuilder().addNames("value");
        schema.getStructBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypesBuilder().getI64Builder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        var detail = ExchangeInputDetail.newBuilder().setPlanNodeId(PLAN_NODE).setExchangeId(EXCHANGE_ID)
                .setUpstreamStageId(producer.request().getAttempt().getStageId()).setDownstreamStageId(4)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON).setPartitionCount(1)
                .setOutputSchema(schema).setBatchSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH).setRequireComplete(true);
        var envelope = RelationDetailEnvelope.newBuilder().setSchemaVersion(1).setExchangeInput(detail).build();
        var plan = Plan.newBuilder();
        plan.addRelationsBuilder().getRootBuilder().addNames("value").getInputBuilder()
                .getExtensionLeafBuilder().setDetail(Any.pack(envelope));
        var base = ActualNativeTaskFixture.planTaskRequest(true, plan.build().toByteArray());
        var producerAttempt = producer.request().getAttempt();
        var attempt = base.getAttempt().toBuilder().setQueryOrJobId(producerAttempt.getQueryOrJobId())
                .setTaskId("actual-external-consumer-" + UUID.randomUUID()).setStageId(4)
                .setPlanEpoch(producerAttempt.getPlanEpoch()).setExecutionOwner(producerAttempt.getExecutionOwner());
        var result = base.toBuilder().setAttempt(attempt).build();
        CanonicalTaskRequests.validateForAdmission(result);
        return result;
    }

    static byte[] arrowBatch(int rows) throws Exception {
        var field = new Field("value",
                FieldType.notNullable(new ArrowType.Int(64, true)), List.of());
        try (var allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
             var root = VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(List.of(field)), allocator);
             var output = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
            var vector = (BigIntVector) root.getVector(0);
            vector.allocateNew(rows);
            for (int row = 0; row < rows; row++) vector.set(row, row);
            vector.setValueCount(rows);
            root.setRowCount(rows);
            writer.start();
            writer.writeBatch();
            writer.end();
            return output.toByteArray();
        }
    }
}
