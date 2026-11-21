package org.pangrova.protocol.function.selection;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.FunctionPhysicalTypeProjector;
import org.pangrova.protocol.function.FunctionSignatureBinder;
import org.pangrova.protocol.function.generated.GeneratedSpecialTypePrograms;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.FunctionImplementationContract;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Binds an actual owned structural programme; never synthesizes a scalar signature. */
final class FunctionSpecialTypeBinder {
    private record Value(Type declared, Type normalized, String kind, List<String> names) {
        private Value { names = List.copyOf(names); }
    }

    private final FunctionPhysicalTypeProjector projector;

    FunctionSpecialTypeBinder(FunctionPhysicalTypeProjector projector) {
        this.projector = Objects.requireNonNull(projector, "projector");
    }

    FunctionSignatureBinder.Binding bind(FunctionImplementationContract implementation,
                                         NamedStruct arguments, Type declaredTarget) {
        SubstraitRelationLayout.topLevelNames(arguments);
        List<Value> values = new ArrayList<>();
        for (int ordinal = 0; ordinal < arguments.getStruct().getTypesCount(); ordinal++) {
            NamedStruct field = SubstraitRelationLayout.projectSchema(arguments, List.of(ordinal));
            values.add(value(field.getStruct().getTypes(0), field.getNamesList().subList(1, field.getNamesCount())));
        }
        Value target = declaredTarget == null ? null : value(declaredTarget, List.of());
        var output = GeneratedSpecialTypePrograms.evaluate(implementation.getSpecialTypeProgram(),
                List.copyOf(values), target, new Types());
        return new FunctionSignatureBinder.Binding(output.declared(), Optional.empty());
    }

    private Value value(Type type, List<String> names) {
        var projected = projector.project(type);
        return new Value(type, projected.bindingType(), projected.physicalType().getRuntimeTag(), names);
    }

    private final class Types implements GeneratedSpecialTypePrograms.Types<Value> {
        @Override
        public String kind(Value type) {
            return type.kind();
        }

        @Override
        public boolean equal(Value left, Value right) {
            // Native Type::operator== includes nested ROW names. Runtime
            // equivalence used for declared outputs is a different relation.
            return left.normalized().equals(right.normalized()) && left.names().equals(right.names());
        }

        @Override
        public Value booleanType() {
            return value(Type.newBuilder().setBool(Type.Boolean.newBuilder()
                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build(), List.of());
        }

        @Override
        public Value row(List<Value> children) {
            var row = Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
            List<String> names = new ArrayList<>();
            for (Value child : children) {
                row.addTypes(child.declared());
                names.add("");
                names.addAll(child.names());
            }
            return value(Type.newBuilder().setStruct(row).build(), names);
        }
    }
}
