package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Real canonical binding with a minimal connector-owned codec for commit-path tests. */
final class CanonicalWriteCommitTestBinding {

    private static final ConnectorOutputHandleCodec CODEC = new TestCodec();

    private CanonicalWriteCommitTestBinding() {}

    static byte[] create(CatalogHandle catalog, ConnectorOutputTableHandle handle) {
        return ConnectorWriteBindingFactory.create(catalog, handle).toByteArray();
    }

    static ConnectorOutputHandleCodec codec() {
        return CODEC;
    }

    private record DecodedHandle(
            SchemaTableName table, String mode, long baseSnapshotId)
            implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() {
            return Optional.of(table);
        }
        @Override public String writeMode() {
            return mode;
        }
        @Override public OptionalLong writeBaseSnapshotId() {
            return baseSnapshotId < 0
                    ? OptionalLong.empty() : OptionalLong.of(baseSnapshotId);
        }
    }

    private static final class TestCodec implements ConnectorOutputHandleCodec {
        @Override public String typeUrl() { return "test/distributed-write-output"; }
        @Override public int codecVersion() { return 1; }

        @Override
        public byte[] serialize(ConnectorOutputTableHandle handle) {
            return (handle.targetTable().orElseThrow() + "\n"
                    + handle.writeMode() + "\n"
                    + handle.writeBaseSnapshotId().orElse(-1L))
                    .getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
            if (version != codecVersion()) {
                throw new IllegalArgumentException("Unknown test write-handle version");
            }
            String[] fields = new String(payload, StandardCharsets.UTF_8)
                    .split("\n", -1);
            if (fields.length != 3) {
                throw new IllegalArgumentException("Malformed test write handle");
            }
            int dot = fields[0].indexOf('.');
            if (dot <= 0 || dot == fields[0].length() - 1) {
                throw new IllegalArgumentException("Malformed test table identity");
            }
            return new DecodedHandle(new SchemaTableName(
                    fields[0].substring(0, dot), fields[0].substring(dot + 1)),
                    fields[1], Long.parseLong(fields[2]));
        }

        @Override
        public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
            return List.of("id");
        }
    }
}
