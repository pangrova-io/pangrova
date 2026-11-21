package org.pangrova.protocol.function;

import io.substrait.proto.Type;
import org.pangrova.protocol.function.formula.FunctionTypeFormulaEvaluator;
import org.pangrova.protocol.v2.FunctionSignatureContract;
import org.pangrova.protocol.v2.FunctionTypeFormula;
import org.pangrova.protocol.v2.FunctionTypeVariable;
import org.pangrova.protocol.v2.FunctionVariableKind;

import java.util.HashMap;
import java.util.Map;

/** Per-call registered variable assignment; never shared between overload candidates. */
final class FunctionSignatureBindings {
    private final Map<String, FunctionTypeVariable> declarations = new HashMap<>();
    private final Map<String, Type> types = new HashMap<>();
    private final Map<String, Long> integers = new HashMap<>();
    private final FunctionPhysicalTypeProjector projector;

    FunctionSignatureBindings(FunctionSignatureContract signature, FunctionPhysicalTypeProjector projector) {
        this.projector = projector;
        if (signature.getVariablesCount() > 256 || !signature.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Registered signature variables exceed their bounds");
        }
        for (var declaration : signature.getVariablesList()) {
            if (declaration.getName().isBlank() || !declaration.getUnknownFields().asMap().isEmpty()
                    || declaration.getKind() == FunctionVariableKind.FUNCTION_VARIABLE_KIND_UNSPECIFIED
                    || declaration.getKind() == FunctionVariableKind.UNRECOGNIZED
                    || declarations.putIfAbsent(declaration.getName(), declaration) != null) {
                throw new IllegalArgumentException("Unknown or duplicate registered signature variable");
            }
        }
    }

    boolean bindType(String name, Type actual) {
        var declaration = require(name, FunctionVariableKind.FUNCTION_VARIABLE_KIND_TYPE);
        var projected = projector.project(actual);
        if ((declaration.getKnownTypesOnly() && !projected.known())
                || (declaration.getOrderableTypesOnly() && !projected.orderable())
                || (declaration.getComparableTypesOnly() && !projected.comparable())) return false;
        Type previous = types.putIfAbsent(name, projected.bindingType());
        return previous == null || previous.equals(projected.bindingType());
    }

    boolean bindInteger(String name, int value) {
        var declaration = require(name, FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER);
        // The actual Native input binder checks digit-only constraints, not derived formulas.
        if (declaration.hasInputIntegerConstant() && declaration.getInputIntegerConstant() != value) return false;
        Long previous = integers.putIfAbsent(name, (long) value);
        return previous == null || previous == value;
    }

    Type type(String name) {
        require(name, FunctionVariableKind.FUNCTION_VARIABLE_KIND_TYPE);
        Type result = types.get(name);
        if (result == null) throw new IllegalArgumentException("Unbound registered type variable: " + name);
        return result;
    }

    int resolveInteger(FunctionTypeFormula formula) {
        if (formula.getOperation()
                == org.pangrova.protocol.v2.FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE) {
            String name = formula.getVariableName();
            var declaration = require(name, FunctionVariableKind.FUNCTION_VARIABLE_KIND_INTEGER);
            if (!integers.containsKey(name)) {
                if (!declaration.hasFormula()) {
                    throw new IllegalArgumentException("Unbound registered integer: " + name);
                }
                // No recursive dependency inference: the actual registered result parameter
                // traversal must already have assigned every dependency.
                integers.put(name, (long) FunctionTypeFormulaEvaluator.evaluateParameter(
                        declaration.getFormula(), integers));
            }
        }
        return FunctionTypeFormulaEvaluator.evaluateParameter(formula, integers);
    }

    private FunctionTypeVariable require(String name, FunctionVariableKind kind) {
        var declaration = declarations.get(name);
        if (declaration == null || declaration.getKind() != kind) {
            throw new IllegalArgumentException("Signature slot references an undeclared variable: " + name);
        }
        return declaration;
    }
}
