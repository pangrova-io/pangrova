package org.pangrova.protocol.retrieval;

import java.util.List;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.CandidateIdentityKind;
import org.pangrova.common.api.annotation.Internal;

/** Canonical candidate identity, distinct from generic deterministic value ordering. */
@Internal
public final class RetrievalCandidateIdentity {
    public static final int SCHEMA_VERSION = 2;
    public static final List<String> PHYSICAL_FIELDS = List.of(
            "$pangrova$source_id", "$pangrova$source_snapshot",
            "$pangrova$source_object_id", "$pangrova$row_position");

    private RetrievalCandidateIdentity() {}

    /** Returns whether the planner retained all four reserved physical fields. */
    public static boolean hasPhysicalFields(NamedStruct schema) {
        int count = schema.getStruct().getTypesCount();
        return schema.getNamesCount() == count && count >= 4
                && schema.getNamesList().subList(count - 4, count).equals(PHYSICAL_FIELDS);
    }

    /** Validates physical channels using the canonical types, never displayed values. */
    public static CandidateIdentityKind kind(
            NamedStruct requested, List<Integer> stableKeys) {
        if (!requested.hasStruct()
                || requested.getNamesCount() != requested.getStruct().getTypesCount()) {
            throw new IllegalArgumentException("Retrieval identity requires a flat canonical schema");
        }
        if (stableKeys.isEmpty() || stableKeys.stream().distinct().count() != stableKeys.size()
                || stableKeys.stream().anyMatch(index -> index < 0
                        || index >= requested.getStruct().getTypesCount())) {
            throw new IllegalArgumentException("Retrieval stable identity channels are invalid");
        }
        int start = physicalStart(requested);
        if (start < 0) {
            if (requested.getNamesList().stream().anyMatch(PHYSICAL_FIELDS::contains)) {
                throw new IllegalArgumentException("Physical retrieval identity fields are incomplete");
            }
            return CandidateIdentityKind.CANDIDATE_IDENTITY_KIND_VALUE_FIELDS;
        }
        List<Integer> expected = List.of(start, start + 1, start + 2, start + 3);
        if (!stableKeys.equals(expected)) {
            throw new IllegalArgumentException("Physical retrieval identity channels are incomplete");
        }
        for (int index = start; index < start + 4; index++) {
            Type type = requested.getStruct().getTypes(index);
            boolean valid = index == start + 3
                    ? type.hasI64() && type.getI64().getNullability() == Type.Nullability.NULLABILITY_REQUIRED
                    : type.hasString() && type.getString().getNullability() == Type.Nullability.NULLABILITY_REQUIRED;
            if (!valid) {
                throw new IllegalArgumentException("Physical retrieval identity types are invalid");
            }
        }
        return CandidateIdentityKind.CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW;
    }

    private static int physicalStart(NamedStruct requested) {
        int start = -1;
        for (int index = 0; index <= requested.getNamesCount() - 4; index++) {
            if (requested.getNamesList().subList(index, index + 4).equals(PHYSICAL_FIELDS)) {
                if (start >= 0) throw new IllegalArgumentException("Physical retrieval fields are duplicated");
                start = index;
            }
        }
        return start;
    }
}
