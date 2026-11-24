package org.pangrova.planner.analysis;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import io.substrait.type.proto.TypeProtoConverter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.planner.substrait.TypeMapper;
import org.pangrova.protocol.function.catalog.FunctionCatalogView;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

final class DecimalAggregateExchange {
    private static final TypeMapper TYPES = new TypeMapper();

    private static final int DECIMAL_BYTES = 16;
    // Each global partial Driver can publish one independent Arrow page.
    // Charge its schema, record-batch framing and per-field metadata in
    // addition to four int64 state values, binary offsets and alignment.
    private static final int ESTIMATED_STATE_PAGE_BYTES = 512;
    private static final int ESTIMATED_STATE_COLUMN_BYTES = 104;

    private DecimalAggregateExchange() {}

    static List<AggregateExchangeBinding> select(LogicalAggregate aggregate,
            Supplier<List<FunctionCatalogView>> catalogueSource, int producers, int maxDrivers) {
        var calls = aggregate.getAggCallList();
        if (producers <= 1 || maxDrivers <= 0 || aggregate.getGroupCount() != 0
                || aggregate.getGroupSets().size() != 1 || calls.isEmpty()) {
            return List.of();
        }
        var input = aggregate.getInput();
        var inputFields = input.getRowType().getFieldList();
        var argumentOrdinals = new HashSet<Integer>();
        for (var call : calls) {
            if (!decimalMeasure(call)) {
                return List.of();
            }
            int ordinal = call.getArgList().get(0);
            if (inputFields.get(ordinal).getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
                return List.of();
            }
            argumentOrdinals.add(ordinal);
        }
        Double inputRows = aggregate.getCluster().getMetadataQuery().getRowCount(input);
        long stateRows = (long) producers * maxDrivers;
        double stateBytes = (double) stateRows
                * (ESTIMATED_STATE_PAGE_BYTES + (long) calls.size() * ESTIMATED_STATE_COLUMN_BYTES);
        if (inputRows == null || !Double.isFinite(inputRows) || inputRows <= stateRows
                || inputRows * argumentOrdinals.size() * DECIMAL_BYTES <= stateBytes) {
            return List.of();
        }

        var catalogues = catalogueSource.get();
        if (catalogues.isEmpty()) {
            return List.of();
        }
        var converter = new TypeProtoConverter(new ExtensionCollector());
        var bindings = new ArrayList<AggregateExchangeBinding>(calls.size());
        for (var call : calls) {
            var inputType = inputFields.get(call.getArgList().get(0)).getType();
            var raw = NamedStruct.newBuilder().addNames("argument_0")
                    .setStruct(Type.Struct.newBuilder().addTypes(converter.toProto(TYPES.map(inputType)))).build();
            var kind = FunctionKind.FUNCTION_KIND_AGGREGATE;
            var declaration = FunctionBindingDeclarations.sqlBinding(kind, "", call.getAggregation().getKind().name())
                    .orElseThrow();
            var urn = FunctionBindingDeclarations.canonicalUrn(kind, declaration.namespace(), declaration.name());
            var binding = AggregateExchangeBinding.select(catalogues, urn, declaration.name(), raw);
            if (binding.isEmpty()) {
                return List.of();
            }
            var selected = binding.orElseThrow();
            if (!selected.state().hasBinary() || !sameDecimalShape(selected.result(), call)) {
                return List.of();
            }
            bindings.add(selected);
        }
        return List.copyOf(bindings);
    }

    private static boolean decimalMeasure(AggregateCall call) {
        var kind = call.getAggregation().getKind();
        return (kind == SqlKind.SUM || kind == SqlKind.AVG) && call.getArgList().size() == 1
                && call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
                && !call.isDistinct() && !call.isApproximate() && !call.ignoreNulls()
                && call.distinctKeys == null && !call.hasCollation() && call.rexList.isEmpty();
    }

    private static boolean sameDecimalShape(Type actual, AggregateCall call) {
        return actual.hasDecimal() && actual.getDecimal().getPrecision() == call.getType().getPrecision()
                && actual.getDecimal().getScale() == call.getType().getScale();
    }
}
