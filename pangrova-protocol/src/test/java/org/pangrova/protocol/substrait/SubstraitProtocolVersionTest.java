package org.pangrova.protocol.substrait;

import io.substrait.proto.Plan;
import io.substrait.proto.Version;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubstraitProtocolVersionTest {
    @Test
    void stampedVersionSurvivesActualProtobufRoundTrip() throws Exception {
        var builder = Plan.newBuilder();
        SubstraitProtocolVersion.stamp(builder);
        var plan = Plan.parseFrom(builder.build().toByteArray());
        assertThat(plan.getVersion()).isEqualTo(SubstraitProtocolVersion.build());
        assertThatCode(() -> SubstraitProtocolVersion.validate(plan)).doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingVersionAndUnidentifiedProducer() {
        assertThatThrownBy(() -> SubstraitProtocolVersion.validate(Plan.getDefaultInstance()))
                .isInstanceOf(SubstraitVersionMismatchException.class).hasMessageContaining("missing required Version");
        assertThatThrownBy(() -> SubstraitProtocolVersion.validate(Plan.newBuilder()
                .setVersion(Version.newBuilder().setMajorNumber(SubstraitProtocolVersion.SPEC_MAJOR)).build()))
                .isInstanceOf(SubstraitVersionMismatchException.class).hasMessageContaining("producer is empty");
    }

    @Test
    void majorMismatchKeepsStructuredErrorWithoutPlannerExceptionDependency() {
        var plan = Plan.newBuilder().setVersion(SubstraitProtocolVersion.build().toBuilder()
                .setMajorNumber(SubstraitProtocolVersion.SPEC_MAJOR + 1)).build();
        assertThatThrownBy(() -> SubstraitProtocolVersion.validate(plan))
                .isInstanceOf(SubstraitVersionMismatchException.class)
                .hasMessageContaining("major-version mismatch")
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE));
        assertThat(SubstraitVersionMismatchException.class.getSuperclass()).isEqualTo(PangrovaException.class);
    }

    @Test
    void currentCompatibilityPolicyAcceptsMinorAndPatchDifferences() {
        var plan = Plan.newBuilder().setVersion(SubstraitProtocolVersion.build().toBuilder()
                .setMinorNumber(SubstraitProtocolVersion.SPEC_MINOR + 1)
                .setPatchNumber(SubstraitProtocolVersion.SPEC_PATCH + 1)).build();
        assertThatCode(() -> SubstraitProtocolVersion.validate(plan)).doesNotThrowAnyException();
    }
}
