package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.CharType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.IntegerType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceOutputHandleCodecTest {
    @Test
    void preservesExactStructuralTypesAndCanonicalAllocation() {
        List<Type> types = List.of(new VarcharType(30), new CharType(20), new DecimalType(19, 3),
                new VectorType(3, VectorType.ElementType.DOUBLE), new ArrayType(new VarcharType(40)),
                new MapType(IntegerType.INSTANCE, new ArrayType(new DecimalType(20, 4))),
                new RowType(List.of(new RowField("text", new CharType(17)), new RowField("id", IntegerType.INSTANCE))));
        List<LanceColumn> columns = java.util.stream.IntStream.range(0, types.size()).mapToObj(index ->
                LanceColumn.from(new ConnectorTableMetadata.Column("col" + index,
                        PangrovaTypeMapping.toPangrovaType(types.get(index)), OptionalInt.empty(), types.get(index)))).toList();
        LanceTableHandle table = new LanceTableHandle("lance_catalog", "tenant-1", new SchemaTableName("db", "vectors"),
                "table-1", "s3://warehouse/table-1.lance", columns, Map.of("b", "2", "a", "1"), new byte[32], 7, 2, 20);
        var allocation = new NativeWriteAllocation(table.datasetUri(), NativeWriteAllocation.Operation.APPEND,
                7, table.schemaFence(), "write-1", "staging/write-1", "generation-1", columns.stream().map(LanceColumn::name).toList());
        var handle = new LanceOutputHandle(table, allocation, "INSERT");
        var codec = new LanceOutputHandleCodec();
        byte[] bytes = codec.serialize(handle);
        var decoded = (LanceOutputHandle) codec.deserialize(1, bytes);
        assertThat(codec.serialize(decoded)).containsExactly(bytes);
        assertThat(decoded.table().columns().stream().map(column -> column.metadata().parameterizedType()).toList())
                .containsExactlyElementsOf(types);
        assertThat(decoded.nativeWriteAllocation().orElseThrow().writeId()).isEqualTo("write-1");
        assertThatThrownBy(() -> codec.deserialize(2, bytes)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.deserialize(1, java.util.Arrays.copyOf(bytes, bytes.length - 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.deserialize(1, (new String(bytes, java.nio.charset.StandardCharsets.UTF_8) + " {}").getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static LanceTableHandle table(String tenant, String name) {
        return new LanceTableHandle("lance_catalog", tenant, new SchemaTableName("db", name),
                java.util.UUID.randomUUID().toString(), "s3://warehouse/" + name + ".lance",
                List.of(LanceColumn.from(new ConnectorTableMetadata.Column("id", PangrovaType.INTEGER))),
                Map.of(), new byte[32], 0, 1, 0);
    }

    static LanceOutputHandle output(LanceTableHandle table) {
        return new LanceOutputHandle(table, new NativeWriteAllocation(table.datasetUri(),
                table.version() == 0 ? NativeWriteAllocation.Operation.CREATE : NativeWriteAllocation.Operation.APPEND,
                table.version(), table.schemaFence(), java.util.UUID.randomUUID().toString(),
                "staging/write-1", "generation-1", table.columns().stream().map(LanceColumn::name).toList()), "INSERT");
    }
}
