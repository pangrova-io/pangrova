package org.pangrova.planner.extension;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.protocol.v2.EnforceSingleRowDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

/** A complete input returns its only value, or SQL NULL when empty. */
public record EnforceSingleRowRelDetail(int inputOrdinal) implements Extension.SingleRelDetail {

    @Override
    public Type.Struct deriveRecordType(Rel input) {
        var value = input.getRecordType().fields().get(inputOrdinal);
        return TypeCreator.REQUIRED.struct(List.of(value.withNullable(true)));
    }

    @Override
    public Any toProto(RelProtoConverter converter) {
        var detail = EnforceSingleRowDetail.newBuilder().setInputOrdinal(inputOrdinal);
        return Any.pack(RelationDetailEnvelope.newBuilder().setSchemaVersion(1)
                .setEnforceSingleRow(detail).build());
    }
}
