package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.scan.PaimonNativeMergeAdmission;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual pinned SDK factories establish the Native field-aggregation contract. */
class PaimonSdkAggregationSemanticsTest {
    @Test
    void listaggAppendsInputsAfterTheChronologicalAccumulator() {
        var options = new Options();
        options.setString("fields.value.aggregate-function", "listagg");
        var schema = RowType.of(new org.apache.paimon.types.DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                new String[] {"id", "value"});
        var merge = AggregateMergeFunction.factory(options, schema, List.of("id")).create(schema);
        merge.reset();
        String[] values = {"", "alice", "", "bob", "charlie"};
        for (int sequence = 0; sequence < values.length; sequence++) {
            merge.add(new KeyValue().replace(GenericRow.of(1L), sequence + 1L, RowKind.INSERT,
                    GenericRow.of(1L, BinaryString.fromString(values[sequence]))));
        }
        assertThat(merge.getResult().value().getString(1).toString()).isEqualTo("alice,bob,charlie");
    }

    @Test
    void countIsNotAnSdkFieldAggregatorAndCannotBeAdmittedToNative() {
        assertThatThrownBy(() -> FieldAggregatorFactory.create(DataTypes.BIGINT(), "value", "count",
                new CoreOptions(new Options()))).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("count");
        assertThatThrownBy(() -> PaimonNativeMergeAdmission.validateAggregation("value", "COUNT",
                DataTypes.BIGINT(), new CoreOptions(new Options())))
                .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                .hasMessageContaining("aggregate-function=COUNT");
    }
}
