package org.pangrova.ai.worker.function;

import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.function.declaration.GovernedArgumentLayout;
import org.pangrova.protocol.function.GovernedBindingProjection;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.context.GovernedAiBindingProjector;
import org.pangrova.ai.worker.invocation.AiInvocationService;

import java.util.Base64;
import java.util.List;

/** Named access to the sole exact callback layout, never inferred from row cardinality. */
final class GovernedAiBindingArguments {
    private final GovernedArgumentLayout layout;
    private final List<ColumnValue> arguments;
    private final int offset;
    private final OperatorBinding admitted;

    GovernedAiBindingArguments(List<ColumnValue> arguments, List<List<String>> publicSignatures) {
        layout = FunctionBindingDeclarations.governedLayout("AI_MODEL");
        this.arguments = List.copyOf(arguments);
        offset = arguments.size() - layout.arguments().size();
        if (offset <= 0 || publicSignatures.stream().noneMatch(signature -> signature.size() == offset)) {
            throw new IllegalArgumentException("AI callback requires its exact declared public and binding arguments");
        }
        admitted = validateAdmitted();
    }

    GovernedAiBindingArguments(List<ColumnValue> arguments, String layoutId) {
        layout = FunctionBindingDeclarations.governedLayout(layoutId);
        this.arguments = List.copyOf(arguments);
        offset = 0;
        if (arguments.size() != layout.arguments().size()) {
            throw new IllegalArgumentException("Internal callback requires its exact declared argument layout");
        }
        admitted = validateAdmitted();
    }

    List<ColumnValue> publicArguments() {
        return List.copyOf(arguments.subList(0, offset));
    }

    String text(String name) {
        ColumnValue value = arguments.get(offset + layout.ordinal(name));
        if (value instanceof ColumnValue.StringValue text && text.value() != null && !text.value().isBlank()) {
            return text.value();
        }
        throw new IllegalArgumentException(name + " must be a non-blank VARCHAR");
    }

    String nullableText(String name) {
        ColumnValue value = arguments.get(offset + layout.ordinal(name));
        if (value instanceof ColumnValue.NullValue) return null;
        if (value instanceof ColumnValue.StringValue text) return text.value();
        throw new IllegalArgumentException(name + " must be VARCHAR or NULL");
    }

    long unsigned(String name) {
        String encoded = text(name);
        if (!encoded.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(name + " must be a canonical nonnegative decimal integer");
        }
        try {
            return Long.parseLong(encoded);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " exceeds Java accounting domain", failure);
        }
    }

    byte[] signature(String name) {
        try {
            String encoded = text(name);
            byte[] value = Base64.getDecoder().decode(encoded);
            if (value.length != 32 || !Base64.getEncoder().encodeToString(value).equals(encoded)) {
                throw new IllegalArgumentException("Expected canonical Base64 SHA-256");
            }
            return value;
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(name + " must be canonical Base64 SHA-256", failure);
        }
    }

    BoundAiInvocation binding(AiOperation operation) {
        BoundAiInvocation binding = GovernedAiBindingProjector.fromAdmitted(admitted);
        if (binding.operation() != operation) {
            throw new IllegalArgumentException("Callback operation differs from the admitted capability");
        }
        return binding;
    }

    private OperatorBinding validateAdmitted() {
        OperatorBinding binding = AiInvocationService.admittedBinding(text("invocation_id"));
        List<String> encoded = layout.arguments().stream().map(argument ->
                argument.source().equals("VALUE_EXPRESSION") ? nullableText(argument.name()) : text(argument.name()))
                .toList();
        GovernedBindingProjection.validateValues(layout.id(), binding, encoded);
        return binding;
    }
}
