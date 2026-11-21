package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionPhysicalTypeProjection;
import org.pangrova.protocol.v2.FunctionTypeFormula;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.FunctionVariableKind;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Strict bridge-shape validation; malformed metadata never defaults to support. */
final class FunctionProjectionValidation {
    private static final int MAX_FORMULA_NODES = 2048;

    private FunctionProjectionValidation() {}

    static void validateTemplates(FunctionPhysicalTypeProjection projection) {
        Set<String> names = new HashSet<>();
        for (var variable : projection.getVariablesList()) {
            if (!variable.getUnknownFields().asMap().isEmpty() || variable.getName().isBlank()
                    || !names.add(variable.getName())
                    || variable.getKind() != FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER
                    || variable.hasFormula() || variable.hasInputIntegerConstant() || variable.getKnownTypesOnly()
                    || variable.getOrderableTypesOnly() || variable.getComparableTypesOnly()
                    || !variable.getEnumConstraint().isEmpty()) {
                throw new IllegalArgumentException("Bridge parameter is not a unique exact input integer");
            }
        }
        Set<String> bound = validateTemplate(projection.getLogicalType(), names, true);
        if (!bound.equals(names)) throw new IllegalArgumentException("Bridge parameter declaration is not bound once");
        validateTemplate(projection.getBindingType(), names, false);
        projection.getPredicatesList().forEach(formula ->
                validateFormula(formula, names, new int[]{MAX_FORMULA_NODES}, 0));
        Set<List<Integer>> paths = new HashSet<>();
        for (var path : projection.getRecursiveChildrenList()) {
            if (!paths.add(path.getFieldNumbersList())) {
                throw new IllegalArgumentException("Repeated recursive child projection");
            }
        }
        validateNativeParameters(projection);
    }

    private static void validateNativeParameters(FunctionPhysicalTypeProjection projection) {
        Set<List<org.pangrova.protocol.v2.FunctionTypePathStep>> nativeParameters = new HashSet<>();
        if (projection.getNativeIntegerParameterPathsCount() > 256) {
            throw new IllegalArgumentException("Native parameter paths exceed their bound");
        }
        for (var parameter : projection.getNativeIntegerParameterPathsList()) {
            var leaf = FunctionTypePath.read(projection.getBindingType().getSkeleton(), parameter.getPathList());
            if (!parameter.getUnknownFields().asMap().isEmpty() || !nativeParameters.add(parameter.getPathList())
                    || leaf.field().getType() != FieldDescriptor.Type.INT32
                    || projection.getBindingType().getSlotsList().stream().noneMatch(slot ->
                            slot.getPathList().equals(parameter.getPathList()) && slot.hasInteger()
                                    && slot.getInteger().getOperation()
                                    == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE)) {
                throw new IllegalArgumentException(
                        "Native parameter must reference one exact generated integer binding slot");
            }
        }
    }

    private static Set<String> validateTemplate(FunctionTypeTemplate template, Set<String> names, boolean input) {
        if (!template.hasSkeleton() || template.getHomogeneousRow() || template.getFieldNameConstraintsCount() != 0
                || !template.getUnknownFields().asMap().isEmpty() || template.getSlotsCount() > 256) {
            throw new IllegalArgumentException("Bridge template is not a bounded exact canonical skeleton");
        }
        Set<String> bound = new HashSet<>();
        Set<List<org.pangrova.protocol.v2.FunctionTypePathStep>> paths = new HashSet<>();
        for (var slot : template.getSlotsList()) {
            var leaf = FunctionTypePath.read(template.getSkeleton(), slot.getPathList());
            if (!slot.hasInteger() || !slot.getUnknownFields().asMap().isEmpty()
                    || !paths.add(slot.getPathList()) || leaf.field().getType() != FieldDescriptor.Type.INT32) {
                throw new IllegalArgumentException("Bridge substitution is not a unique exact int32 field");
            }
            validateFormula(slot.getInteger(), names, new int[]{MAX_FORMULA_NODES}, 0);
            if (input && (slot.getInteger().getOperation()
                    != FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE
                    || !bound.add(slot.getInteger().getVariableName()))) {
                throw new IllegalArgumentException("Bridge input parameter must bind one actual field directly");
            }
        }
        return Set.copyOf(bound);
    }

    private static void validateFormula(FunctionTypeFormula formula, Set<String> names, int[] remaining, int depth) {
        if (depth > 64 || --remaining[0] < 0 || !formula.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Bridge formula is unknown or exceeds its bounds");
        }
        var operation = formula.getOperation();
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE) {
            validateVariable(formula, names);
            return;
        }
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER) {
            validateInteger(formula);
            return;
        }
        validateOperator(formula, operation);
        formula.getOperandsList().forEach(child -> validateFormula(child, names, remaining, depth + 1));
    }

    private static void validateVariable(FunctionTypeFormula formula, Set<String> names) {
        if (!names.contains(formula.getVariableName())
                || formula.getIntegerValue() != 0 || formula.getOperandsCount() != 0) {
            throw new IllegalArgumentException("Bridge formula has an unbound or malformed variable");
        }
    }

    private static void validateInteger(FunctionTypeFormula formula) {
        if (!formula.getVariableName().isEmpty() || formula.getOperandsCount() != 0) {
            throw new IllegalArgumentException("Bridge formula has a malformed integer");
        }
    }

    private static void validateOperator(FunctionTypeFormula formula, FunctionFormulaOperation operation) {
        if (!formula.getVariableName().isEmpty() || formula.getIntegerValue() != 0
                || operation == FunctionFormulaOperation.UNRECOGNIZED
                || operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_UNSPECIFIED
                || formula.getOperandsCount() != arity(operation)) {
            throw new IllegalArgumentException("Bridge formula has an unknown operation or wrong arity");
        }
    }

    private static int arity(FunctionFormulaOperation operation) {
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_NEGATE) return 1;
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_CONDITIONAL) return 3;
        return 2;
    }
}
