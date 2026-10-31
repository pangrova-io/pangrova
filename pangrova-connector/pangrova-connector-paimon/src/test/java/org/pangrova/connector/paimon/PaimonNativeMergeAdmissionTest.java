package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.scan.PaimonNativeMergeAdmission;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.spi.base.ConnectorException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonNativeMergeAdmissionTest {
    @Test
    void sdkDefaultArrayAndMapPoliciesAreAdmitted() {
        var options = new CoreOptions(new Options());
        assertThatCode(() -> PaimonNativeMergeAdmission.validateAggregation("items", "COLLECT",
                DataTypes.ARRAY(DataTypes.BIGINT()), options)).doesNotThrowAnyException();
        assertThatCode(() -> PaimonNativeMergeAdmission.validateAggregation("items", "MERGE_MAP",
                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()), options)).doesNotThrowAnyException();
    }

    @Test
    void unrepresentedDistinctPolicyFailsBeforeAnyNativeTask() {
        var raw = new Options();
        raw.setString("fields.items.distinct", "true");
        assertThatThrownBy(() -> PaimonNativeMergeAdmission.validateAggregation("items", "COLLECT",
                DataTypes.ARRAY(DataTypes.BIGINT()), new CoreOptions(raw)))
                .isInstanceOfSatisfying(ConnectorException.class,
                        error -> assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION))
                .hasMessageContaining("fields.items.distinct");
    }

    @Test
    void decimalProductCannotBeMisinterpretedAsUnscaledIntegerProduct() {
        assertThatThrownBy(() -> PaimonNativeMergeAdmission.validateAggregation("amount", "PRODUCT",
                DataTypes.DECIMAL(20, 2), new CoreOptions(new Options())))
                .isInstanceOf(ConnectorException.class).hasMessageContaining("PRODUCT on DECIMAL(20, 2)");
    }
}
