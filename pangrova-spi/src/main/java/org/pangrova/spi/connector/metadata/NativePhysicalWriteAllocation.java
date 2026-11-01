package org.pangrova.spi.connector.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.type.DeclaredType;
import org.pangrova.spi.connector.data.type.TypeSignature;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Connector-owned physical-file write authority; contains metadata, never rows or credentials. */
@Experimental
public record NativePhysicalWriteAllocation(
        Identity identity, Schema schema, Policy policy, String allocationPrefix,
        long baseSnapshotId, List<NativePhysicalFileAllocation> files) {
    private static final int MAX_ALLOCATED_FILES = 4096;
    private static final int MAX_SCHEMA_COLUMNS = 4096;
    private static final int MAX_TEXT_LENGTH = 16_384;

    public NativePhysicalWriteAllocation {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(policy, "policy");
        requireText(allocationPrefix, "allocationPrefix");
        if (baseSnapshotId < -1) throw new IllegalArgumentException("Invalid base snapshot");
        files = List.copyOf(files);
        if (files.isEmpty() || files.size() > MAX_ALLOCATED_FILES) {
            throw new IllegalArgumentException("Physical allocation requires a bounded file set");
        }
    }

    public record Identity(String writeId, String generation) {
        public Identity {
            requireText(writeId, "writeId");
            requireText(generation, "generation");
        }
    }

    /** Full immutable field identity/type/nullability; a schema hash alone is insufficient. */
    public record Column(int fieldId, String name, DeclaredType declaredType) {
        public Column {
            if (fieldId < 0) throw new IllegalArgumentException("Negative field ID");
            requireText(name, "column name");
            Objects.requireNonNull(declaredType, "declaredType");
        }
        public TypeSignature logicalType() { return declaredType.signature(); }
        public boolean nullable() { return declaredType.nullable(); }
    }

    /** Schema-only typed IPC metadata from the connector SDK; not a row payload. */
    public record Schema(long schemaId, byte[] physicalSchema, byte[] sha256, List<Column> columns) {
        public Schema {
            if (schemaId < 0) throw new IllegalArgumentException("Negative schema ID");
            physicalSchema = copyBytes(physicalSchema, 16 * 1024 * 1024, "physicalSchema");
            sha256 = copyBytes(sha256, 32, "schema SHA-256");
            if (sha256.length != 32) throw new IllegalArgumentException("Invalid schema SHA-256");
            columns = List.copyOf(columns);
            if (columns.isEmpty() || columns.size() > MAX_SCHEMA_COLUMNS) {
                throw new IllegalArgumentException("Physical schema requires bounded columns");
            }
            if (columns.stream().map(Column::fieldId).distinct().count() != columns.size()
                    || columns.stream().map(Column::name).distinct().count() != columns.size()) {
                throw new IllegalArgumentException("Duplicate physical field identity");
            }
        }

        @Override public byte[] physicalSchema() { return physicalSchema.clone(); }
        @Override public byte[] sha256() { return sha256.clone(); }
    }

    public record Policy(String writeMode, List<Integer> partitionFields, List<Integer> primaryKeyFields,
                         List<Integer> bucketKeyFields, int bucketCount, long writerBufferHintBytes,
                         Map<String, String> options) {
        private static final int MIN_BUCKET_COUNT = -2;

        public Policy {
            if (!List.of("INSERT", "OVERWRITE", "DELETE").contains(writeMode)) {
                throw new IllegalArgumentException("Unsupported physical write mode");
            }
            partitionFields = List.copyOf(partitionFields);
            primaryKeyFields = List.copyOf(primaryKeyFields);
            bucketKeyFields = List.copyOf(bucketKeyFields);
            options = Map.copyOf(options);
            if (bucketCount < MIN_BUCKET_COUNT || bucketCount == 0) {
                throw new IllegalArgumentException("Invalid bucket policy");
            }
            if (writerBufferHintBytes <= 0) {
                throw new IllegalArgumentException("Physical writer requires an explicit positive buffer hint");
            }
        }
    }

    static byte[] copyBytes(byte[] value, int limit, String name) {
        byte[] copy = Objects.requireNonNull(value, name).clone();
        if (copy.length == 0 || copy.length > limit) throw new IllegalArgumentException("Invalid " + name);
        return copy;
    }

    static void requireText(String value, String name) {
        if (value == null || value.isBlank() || value.length() > MAX_TEXT_LENGTH) {
            throw new IllegalArgumentException("Invalid " + name);
        }
    }
}
