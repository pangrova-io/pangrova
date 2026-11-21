package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.formula.FunctionTypeFormulaEvaluator;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionTypePathStep;
import org.pangrova.protocol.v2.FunctionTypeTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

/** Exact canonical template unification, without casts or inferred SQL categories. */
final class FunctionTypeTemplateMatcher {
    private final FunctionPhysicalTypeProjector projector;
    private int remaining = 4096;

    FunctionTypeTemplateMatcher(FunctionPhysicalTypeProjector projector) {
        this.projector = projector;
    }

    boolean match(FunctionTypeTemplate template, Type actual, FunctionSignatureBindings bindings,
                  Function<List<FunctionTypePathStep>, String> fieldNames) {
        return match(template, actual, bindings, fieldNames, 0);
    }

    private boolean match(FunctionTypeTemplate template, Type actual, FunctionSignatureBindings bindings,
                          Function<List<FunctionTypePathStep>, String> fieldNames, int depth) {
        consume(template, depth);
        if (template.hasTypeVariable()) return bindings.bindType(template.getTypeVariable(), actual);
        if (template.hasAnyType()) {
            if (!template.getAnyType().getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Registered ANY marker has unknown fields");
            }
            projector.project(actual);
            return true;
        }
        if (template.hasOpaqueRegisteredType()) {
            FunctionRegisteredTypeAst.validate(template.getOpaqueRegisteredType());
            // Preserved actual registration evidence is not authority to execute an
            // unprojected runtime extension as an arbitrary canonical SQL type.
            return false;
        }
        if (!template.hasSkeleton()) throw new IllegalArgumentException("Registered template has no explicit root");
        Type normalized = projector.project(actual).bindingType();
        Type skeleton = template.getSkeleton();
        if (!FunctionTypePath.kind(skeleton).equals(FunctionTypePath.kind(normalized))) return false;
        if (template.getHomogeneousRow()) return homogeneous(template, normalized, bindings, fieldNames, depth);
        return matchSkeleton(template, normalized, bindings, fieldNames, depth);
    }

    private boolean matchSkeleton(FunctionTypeTemplate template, Type actual, FunctionSignatureBindings bindings,
                                  Function<List<FunctionTypePathStep>, String> names, int depth) {
        Message normalized = actual;
        var paths = new HashSet<List<FunctionTypePathStep>>();
        for (var slot : template.getSlotsList()) {
            if (!slot.getUnknownFields().asMap().isEmpty() || !paths.add(slot.getPathList())) {
                throw new IllegalArgumentException("Unknown or duplicate registered template slot");
            }
            var pattern = FunctionTypePath.read(template.getSkeleton(), slot.getPathList());
            var value = FunctionTypePath.read(actual, slot.getPathList());
            if (slot.hasType()) {
                if (!(value.value() instanceof Type child) || !(pattern.value() instanceof Type)) {
                    throw new IllegalArgumentException("Registered type slot is not a canonical Type field");
                }
                if (!match(slot.getType(), child, bindings, nestedNames(names, slot.getPathList()), depth + 1)) {
                    return false;
                }
            } else if (slot.hasInteger()) {
                if (!matchInteger(slot.getInteger(), value, bindings)) return false;
            } else {
                throw new IllegalArgumentException("Unprojected enum or absent template slot cannot execute");
            }
            normalized = FunctionTypePath.replace(normalized, slot.getPathList(), pattern.value());
        }
        for (var constraint : template.getFieldNameConstraintsList()) {
            if (!constraint.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Registered row-name constraint has unknown fields");
            }
            if (!constraint.getName().equals(names.apply(constraint.getPathList()))) return false;
        }
        return normalized.equals(template.getSkeleton());
    }

    private boolean homogeneous(FunctionTypeTemplate template, Type actual, FunctionSignatureBindings bindings,
                                Function<List<FunctionTypePathStep>, String> names, int depth) {
        if (!template.getSkeleton().hasStruct() || template.getSkeleton().getStruct().getTypesCount() != 1
                || template.getSlotsCount() != 1 || !template.getSlots(0).hasType()) {
            throw new IllegalArgumentException("Homogeneous registered ROW needs one repeated type pattern");
        }
        if (!actual.hasStruct()) return false;
        var pattern = template.getSlots(0).getType();
        for (Type child : actual.getStruct().getTypesList()) {
            // SDK homogeneous ROW binding deliberately ignores individual field names.
            if (!match(pattern, child, bindings, ignored -> {
                throw new IllegalArgumentException("Homogeneous ROW cannot infer named-field constraints");
            }, depth + 1)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchInteger(org.pangrova.protocol.v2.FunctionTypeFormula formula,
                                        FunctionTypePath.Leaf leaf, FunctionSignatureBindings bindings) {
        if (leaf.field().getType() != FieldDescriptor.Type.INT32 || !(leaf.value() instanceof Integer value)) {
            throw new IllegalArgumentException("Registered integer slot is not a canonical int32 field");
        }
        if (formula.getOperation() == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE) {
            FunctionTypeFormulaEvaluator.evaluateParameter(formula,
                    java.util.Map.of(formula.getVariableName(), (long) value));
            return bindings.bindInteger(formula.getVariableName(), value);
        }
        if (formula.getOperation() == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER) {
            return FunctionTypeFormulaEvaluator.evaluateParameter(formula, java.util.Map.of()) == value;
        }
        throw new IllegalArgumentException("Registered input parameter must be a variable or literal");
    }

    private static Function<List<FunctionTypePathStep>, String> nestedNames(
            Function<List<FunctionTypePathStep>, String> names, List<FunctionTypePathStep> prefix) {
        return suffix -> {
            var full = new java.util.ArrayList<>(prefix);
            full.addAll(suffix);
            return names.apply(List.copyOf(full));
        };
    }

    private void consume(FunctionTypeTemplate template, int depth) {
        if (--remaining < 0 || depth > 64 || !template.getUnknownFields().asMap().isEmpty()
                || template.getSlotsCount() > 256 || template.getFieldNameConstraintsCount() > 256) {
            throw new IllegalArgumentException("Registered template is unknown or exceeds its bounds");
        }
        if (!template.hasSkeleton() && (template.getSlotsCount() != 0
                || template.getFieldNameConstraintsCount() != 0 || template.getHomogeneousRow())) {
            throw new IllegalArgumentException("Non-skeleton registered template has skeleton metadata");
        }
    }
}
