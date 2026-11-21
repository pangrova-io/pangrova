package org.pangrova.testing.benchmark.jmh;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for Arrow batch operations.
 *
 * <p>Measures the throughput of creating, populating, and reading Arrow VectorSchemaRoot
 * batches. These operations represent the core data path in the query engine where
 * columnar batches are produced by scan operators, transformed by compute operators,
 * and consumed by sink operators.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 2, jvmArgs = {"--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class BatchOperationsBenchmark {

    @Param({"1024", "4096", "8192"})
    private int batchSize;

    private BufferAllocator allocator;
    private Schema schema;
    private VectorSchemaRoot prefilledBatch;

    @Setup(Level.Trial)
    public void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("amount", FieldType.nullable(new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null),
                new Field("quantity", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        prefilledBatch = VectorSchemaRoot.create(schema, allocator);
        fillBatch(prefilledBatch, batchSize);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        prefilledBatch.close();
        allocator.close();
    }

    @Benchmark
    public void createAndFillBatch(Blackhole bh) {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            fillBatch(batch, batchSize);
            bh.consume(batch.getRowCount());
        }
    }

    @Benchmark
    public void readBatchColumns(Blackhole bh) {
        BigIntVector idVector = (BigIntVector) prefilledBatch.getVector("id");
        Float8Vector amountVector = (Float8Vector) prefilledBatch.getVector("amount");
        IntVector quantityVector = (IntVector) prefilledBatch.getVector("quantity");
        VarCharVector nameVector = (VarCharVector) prefilledBatch.getVector("name");

        long sum = 0;
        for (int i = 0; i < prefilledBatch.getRowCount(); i++) {
            sum += idVector.get(i);
            bh.consume(amountVector.get(i));
            bh.consume(quantityVector.get(i));
            bh.consume(nameVector.get(i));
        }
        bh.consume(sum);
    }

    @Benchmark
    public void batchSliceAndTransfer(Blackhole bh) {
        try (VectorSchemaRoot slice = VectorSchemaRoot.create(schema, allocator)) {
            int halfBatch = batchSize / 2;
            slice.allocateNew();

            BigIntVector srcId = (BigIntVector) prefilledBatch.getVector("id");
            BigIntVector dstId = (BigIntVector) slice.getVector("id");
            Float8Vector srcAmount = (Float8Vector) prefilledBatch.getVector("amount");
            Float8Vector dstAmount = (Float8Vector) slice.getVector("amount");

            for (int i = 0; i < halfBatch; i++) {
                dstId.setSafe(i, srcId.get(i));
                dstAmount.setSafe(i, srcAmount.get(i));
            }
            slice.setRowCount(halfBatch);
            bh.consume(slice.getRowCount());
        }
    }

    private static void fillBatch(VectorSchemaRoot batch, int rows) {
        batch.allocateNew();
        BigIntVector idVector = (BigIntVector) batch.getVector("id");
        Float8Vector amountVector = (Float8Vector) batch.getVector("amount");
        IntVector quantityVector = (IntVector) batch.getVector("quantity");
        VarCharVector nameVector = (VarCharVector) batch.getVector("name");

        byte[] namePrefix = "item_name_".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < rows; i++) {
            idVector.setSafe(i, (long) i * 1000 + 1);
            amountVector.setSafe(i, i * 99.95);
            quantityVector.setSafe(i, i % 100 + 1);
            nameVector.setSafe(i, namePrefix);
        }
        batch.setRowCount(rows);
    }
}
