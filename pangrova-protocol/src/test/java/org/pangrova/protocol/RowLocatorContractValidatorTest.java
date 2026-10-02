package org.pangrova.protocol;

import com.google.protobuf.ByteString;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RowLocatorContract;
import org.pangrova.protocol.v2.RowLocatorKind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowLocatorContractValidatorTest {

    @Test
    void acceptsCanonicalPhysicalContract() {
        assertThat(RowLocatorContractValidator.validate(
                BuiltInCapabilityCatalog.physicalRowLocatorContract()))
                .isEqualTo(BuiltInCapabilityCatalog.physicalRowLocatorContract());
    }

    @Test
    void rejectsUnscopedPhysicalPosition() {
        var invalid = BuiltInCapabilityCatalog.physicalRowLocatorContract()
                .toBuilder().setSourceObjectScoped(false).build();
        assertThatThrownBy(() -> RowLocatorContractValidator.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsTypedPrimaryKeyAndRejectsUntypedKey() {
        var typed = RowLocatorContract.newBuilder()
                .setSchemaVersion(1)
                .setKind(RowLocatorKind.ROW_LOCATOR_KIND_PRIMARY_KEY)
                .setSchemaSha256(ByteString.copyFrom(new byte[32]))
                .addKeyTypes(LogicalType.newBuilder()
                        .setCanonicalSql("BIGINT")
                        .setSubstraitType(Type.newBuilder().setI64(Type.I64.newBuilder())))
                .build();
        assertThat(RowLocatorContractValidator.validate(typed)).isEqualTo(typed);

        var untyped = typed.toBuilder()
                .setKeyTypes(0, LogicalType.newBuilder().setCanonicalSql("BIGINT"))
                .build();
        assertThatThrownBy(() -> RowLocatorContractValidator.validate(untyped))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
