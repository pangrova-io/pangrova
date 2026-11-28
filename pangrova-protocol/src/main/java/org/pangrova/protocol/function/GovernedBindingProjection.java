package org.pangrova.protocol.function;

import com.google.protobuf.Message;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.function.declaration.GovernedArgumentLayout;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.OperatorBinding;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Projects exact callback authority from the admitted binding using the sole layout declaration. */
public final class GovernedBindingProjection {
    private GovernedBindingProjection() {}

    /** VALUE_EXPRESSION arguments are data, not task authority, and are deliberately excluded. */
    public static Map<String, GovernedScalarValue> values(String layoutId, OperatorBinding binding) {
        if (binding == null || binding.getPlanNodeId().isBlank()) {
            throw new IllegalArgumentException("Admitted binding requires its exact invocation identity");
        }
        Map<String, GovernedScalarValue> result = new LinkedHashMap<>();
        for (var argument : FunctionBindingDeclarations.governedLayout(layoutId).arguments()) {
            if (!argument.source().equals("VALUE_EXPRESSION")) {
                result.put(argument.name(), project(argument, binding));
            }
        }
        return Map.copyOf(result);
    }

    /** Full declared order is retained even when data expressions are excluded from authority checking. */
    public static void validateValues(String layoutId, OperatorBinding binding, List<String> encodedValues) {
        var layout = FunctionBindingDeclarations.governedLayout(layoutId);
        if (encodedValues == null || encodedValues.size() != layout.arguments().size()) {
            throw new IllegalArgumentException("Callback authority has a different declared arity");
        }
        var expected = values(layoutId, binding);
        for (var argument : layout.arguments()) {
            if (argument.source().equals("VALUE_EXPRESSION")) continue;
            var observed = GovernedScalarDecoder.decode(
                    encodedValues.get(layout.ordinal(argument.name())), argument.encoding());
            if (!expected.get(argument.name()).equals(observed)) {
                throw new IllegalArgumentException("Callback differs from admitted binding: " + argument.name());
            }
        }
    }

    private static GovernedScalarValue project(GovernedArgumentLayout.Argument argument, OperatorBinding binding) {
        return switch (argument.source()) {
            case "OPERATOR_BINDING" -> path(argument, binding);
            case "PLANNER_INVOCATION_ID" -> new GovernedScalarValue.Text(binding.getPlanNodeId());
            case "OUTPUT_VECTOR_DIMENSION" -> new GovernedScalarValue.Unsigned(outputDimension(binding));
            default -> throw new IllegalArgumentException("Unknown governed authority source: " + argument.source());
        };
    }

    private static int outputDimension(OperatorBinding binding) {
        if (!binding.hasModel() || !binding.getModel().hasOutputType()) {
            throw new IllegalArgumentException("Admitted model output type is absent");
        }
        var output = binding.getModel().getOutputType();
        return output.getCanonicalSql().startsWith("VECTOR(") ? VectorLogicalType.requireDimension(output) : 0;
    }

    private static GovernedScalarValue path(GovernedArgumentLayout.Argument argument, OperatorBinding binding) {
        Message current = binding;
        for (int i = 0; i < argument.path().size(); i++) {
            if (!current.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Binding authority contains unknown fields");
            }
            var field = current.getDescriptorForType().findFieldByName(argument.path().get(i));
            if (field == null || field.isRepeated()) {
                throw new IllegalArgumentException("Unknown or repeated binding path");
            }
            if (i + 1 == argument.path().size()) {
                return GovernedScalarDecoder.field(field, current.getField(field), argument.encoding());
            }
            if (field.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
                    || (!argument.allowAbsentMessage() && !current.hasField(field))) {
                throw new IllegalArgumentException("Required binding message is absent or has the wrong type");
            }
            current = (Message) current.getField(field);
        }
        throw new IllegalArgumentException("Binding path cannot be empty");
    }
}
