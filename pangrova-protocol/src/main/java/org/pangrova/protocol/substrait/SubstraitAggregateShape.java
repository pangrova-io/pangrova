package org.pangrova.protocol.substrait;

import io.substrait.proto.AggregateRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/** Grouping expressions use the canonical shared list and explicit set index. */
final class SubstraitAggregateShape {
    private SubstraitAggregateShape() {}

    static NamedStruct describe(AggregateRel aggregate, NamedStruct input) {
        if (aggregate.getGroupingsCount() == 0 && aggregate.getMeasuresCount() == 0) {
            throw new IllegalArgumentException("Aggregate requires grouping sets or measures");
        }
        var references = new LinkedHashSet<Integer>();
        aggregate.getGroupingsList().forEach(group -> references.addAll(group.getExpressionReferencesList()));
        if (references.stream().anyMatch(index -> index < 0 || index >= aggregate.getGroupingExpressionsCount())
                || references.size() != aggregate.getGroupingExpressionsCount()) {
            throw new IllegalArgumentException("Grouping expressions must be referenced exactly within their shared list");
        }
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>();
        for (int index = 0; index < aggregate.getGroupingExpressionsCount(); index++) {
            final int declaredIndex = index;
            var field = SubstraitRelationShapes.expressionField(aggregate.getGroupingExpressions(index), input, "group_" + index);
            if (aggregate.getGroupingsList().stream().anyMatch(group -> !group.getExpressionReferencesList().contains(declaredIndex))) {
                field = new SubstraitSchemaFields.Field(field.name(), SubstraitSchemaFields.withNullability(
                        field.type(), Type.Nullability.NULLABILITY_NULLABLE), field.descendants());
            }
            fields.add(field);
        }
        for (int i = 0; i < aggregate.getMeasuresCount(); i++) {
            var measure = aggregate.getMeasures(i);
            if (measure.hasFilter()) SubstraitRelationShapes.requireBoolean(measure.getFilter(), input);
            fields.add(SubstraitSchemaFields.computed("measure_" + i,
                    SubstraitExpressionTypes.declared(measure.getMeasure().getOutputType())));
        }
        if (aggregate.getGroupingsCount() > 1) fields.add(SubstraitSchemaFields.computed("grouping_set_index",
                SubstraitSchemaFields.primitive("i32", Type.Nullability.NULLABILITY_REQUIRED)));
        return SubstraitSchemaFields.schema(fields);
    }
}
