package org.pangrova.protocol.function.catalog;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionReferenceContract;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionCatalogViewTest {
    @Test
    void capturesEveryActualDefinitionAndRegistrationWithoutErasingOpaqueDiagnostics() {
        var actual = CanonicalCapabilityInventoryTestFixture.inventory();
        var view = new FunctionCatalogView(actual);
        assertThat(view.manifest()).isEqualTo(actual.getFunctionCatalog());
        for (var definition : actual.getFunctionCatalog().getDefinitionsList()) {
            var ref = definition.getRef();
            assertThat(view.definition(ref)).isEqualTo(definition);
            assertThat(view.definition(ref.getKind(), ref.getUrn(), ref.getName())).isEqualTo(definition);
            assertThat(view.isAvailable(ref)).isEqualTo(actual.getFunctionsList().stream()
                    .anyMatch(availability -> availability.getRef().equals(ref)));
        }
        for (var registration : actual.getFunctionCatalog().getRegisteredFunctionsList()) {
            assertThat(view.registration(registration.getKind(), registration.getImplementationName()))
                    .contains(registration);
        }
    }

    @Test
    void constructionAdmitsOnceAndRetainsAnImmutableSnapshotRatherThanItsSourceBuilder() {
        var source = CanonicalCapabilityInventoryTestFixture.inventory().toBuilder();
        var captured = source.build();
        var view = new FunctionCatalogView(captured);
        source.clearFunctionCatalog().clearFunctions();
        assertThat(view.manifest()).isEqualTo(captured.getFunctionCatalog());
        assertThat(view.bindScalar(reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "round"),
                arguments(decimal(12, 3)), Set.of(), Optional.empty()).binding().output().getDecimal().getPrecision())
                .isEqualTo(10);
        assertThatThrownBy(() -> new FunctionCatalogView(source.build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("catalogue is absent");
    }

    @Test
    void exactReferenceRejectsHashUrnNameKindAndUnknownFieldDrift() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "round");
        var wrongHash = ref.toBuilder().setContractSha256(ByteString.copyFrom(new byte[32])).build();
        assertThatThrownBy(() -> view.definition(wrongHash)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from");
        assertThatThrownBy(() -> view.isAvailable(wrongHash)).isInstanceOf(IllegalArgumentException.class);
        for (var changed : new FunctionReferenceContract[]{ref.toBuilder().setUrn("extension:missing").build(),
                ref.toBuilder().setName("missing_round").build(),
                ref.toBuilder().setKind(FunctionKind.FUNCTION_KIND_AGGREGATE).build()}) {
            assertThatThrownBy(() -> view.definition(changed)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("absent");
        }
        var unknown = ref.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        assertThatThrownBy(() -> view.definition(unknown)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from");
    }

    @Test
    void absentOfflineCallbacksRemainUnavailableEvenThoughTheirSoleDefinitionsArePresent() {
        var view = view();
        var callback = view.manifest().getDefinitionsList().stream()
                .filter(definition -> !definition.getRegistrationDependency().isEmpty())
                .filter(definition -> !view.isAvailable(definition.getRef())).findFirst().orElseThrow();
        assertThat(view.definition(callback.getRef())).isEqualTo(callback);
        assertThat(view.registration(callback.getRef().getKind(), callback.getImplementationName())).isEmpty();
        assertThatThrownBy(() -> view.bindScalar(callback.getRef(), arguments(primitive("string")),
                Set.of(), Optional.empty())).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unavailable");
    }

    @Test
    void rewritesAndStructuralLoweringsCannotPretendToBeDirectRegisteredCalls() {
        var view = view();
        for (var definition : view.manifest().getDefinitionsList().stream()
                .filter(value -> value.getRef().getKind() == FunctionKind.FUNCTION_KIND_SCALAR)
                .filter(value -> value.hasRewrite() || !value.getStructuralLowering().isEmpty()).toList()) {
            assertThatThrownBy(() -> view.bindScalar(definition.getRef(), arguments(primitive("i64")),
                    Set.of(), Optional.empty())).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not a direct registered call");
        }
        assertThat(view.manifest().getDefinitionsList()).anyMatch(value -> value.hasRewrite());
        assertThat(view.manifest().getDefinitionsList()).anyMatch(value -> !value.getStructuralLowering().isEmpty());
    }

    @Test
    void actualRoundUsesItsSelectedDecimalFormulaAndDoesNotSearchByRequestedOutput() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "round");
        var one = view.bindScalar(ref, arguments(decimal(12, 3)), Set.of(), Optional.empty());
        assertThat(one.binding().output().getDecimal().getPrecision()).isEqualTo(10);
        assertThat(one.binding().output().getDecimal().getScale()).isZero();
        var differentTarget = view.bindScalar(ref, arguments(decimal(12, 3)), Set.of(), Optional.of(decimal(38, 18)));
        assertThat(differentTarget).isEqualTo(one);
        var two = view.bindScalar(ref, arguments(decimal(12, 3), primitive("i32")), Set.of(1), Optional.empty());
        assertThat(two.binding().output().getDecimal().getPrecision()).isEqualTo(13);
        assertThat(two.binding().output().getDecimal().getScale()).isEqualTo(3);
        var registered = view.registration(FunctionKind.FUNCTION_KIND_SCALAR, "round").orElseThrow();
        assertThat(registered.getImplementationsList()).contains(one.implementation(), two.implementation());
    }

    @Test
    void actualSelectedLikeConstantViolationIsNotAnotherOverloadSearch() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "like");
        var arguments = arguments(primitive("string"), primitive("string"));
        var selected = view.bindScalar(ref, arguments, Set.of(1), Optional.empty());
        assertThat(selected.implementation().getSignature().getConstantArguments(1)).isTrue();
        assertThat(selected.binding().output().hasBool()).isTrue();
        assertThatThrownBy(() -> view.bindScalar(ref, arguments, Set.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires a constant");
    }

    @Test
    void concreteArgumentsRequireRealSignatureExplicitChildNullabilityAndBoundedConstants() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "round");
        assertThatThrownBy(() -> view.bindScalar(ref, arguments(primitive("string")), Set.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No actual scalar signature");
        assertThatThrownBy(() -> view.bindScalar(ref, arguments(decimal(12, 3)), Set.of(1), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outside the actual call");
        var unspecified = decimal(12, 3).toBuilder().setDecimal(decimal(12, 3).getDecimal().toBuilder()
                .setNullability(Type.Nullability.NULLABILITY_UNSPECIFIED)).build();
        assertThatThrownBy(() -> view.bindScalar(ref, arguments(unspecified), Set.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nullability");
    }

    @Test
    void actualAggregateRawBindingsPreserveOrderedSignatureAndExplicitIntermediateOutput() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_AGGREGATE, "sum");
        var raw = arguments(primitive("i64"));
        var selected = view.bindRawAggregate(ref, raw, Set.of(), false);
        var registered = view.registration(FunctionKind.FUNCTION_KIND_AGGREGATE, "sum").orElseThrow();
        assertThat(selected.registration()).isEqualTo(registered);
        assertThat(selected.signature()).isEqualTo(registered.getSignatures(selected.signatureOrdinal()));
        assertThat(selected.binding().output().hasI64()).isTrue();
        var state = view.bindRawAggregate(ref, raw, Set.of(), true);
        assertThat(state.signatureOrdinal()).isEqualTo(selected.signatureOrdinal());
        assertThat(state.binding().output()).isEqualTo(selected.binding().intermediate().orElseThrow());
        assertThatThrownBy(() -> view.bindRawAggregate(ref, arguments(primitive("string")), Set.of(), false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No actual ordered signature");
    }

    @Test
    void actualWindowAndAggregateWithTheSameNameRetainDifferentRegistryKinds() {
        var view = view();
        var window = reference(view, FunctionKind.FUNCTION_KIND_WINDOW, "sum");
        var aggregate = reference(view, FunctionKind.FUNCTION_KIND_AGGREGATE, "sum");
        var selected = view.bindWindow(window, arguments(primitive("i64")), Set.of());
        assertThat(selected.registration().getKind()).isEqualTo(FunctionKind.FUNCTION_KIND_WINDOW);
        assertThat(selected.signature()).isEqualTo(selected.registration().getSignatures(selected.signatureOrdinal()));
        assertThat(view.bindWindow(reference(view, FunctionKind.FUNCTION_KIND_WINDOW, "row_number"),
                arguments(), Set.of()).binding().output().hasI64()).isTrue();
        assertThatThrownBy(() -> view.bindWindow(aggregate, arguments(primitive("i64")), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("wrong exact kind");
        assertThatThrownBy(() -> view.bindRawAggregate(window, arguments(primitive("i64")), Set.of(), false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("wrong exact kind");
    }

    @Test
    void actualCoalesceSpecialPreservesNamedRowEqualityInsteadOfPhysicalEquivalence() {
        var view = view();
        var ref = reference(view, FunctionKind.FUNCTION_KIND_SCALAR, "coalesce");
        var row = Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypes(primitive("i64"))).build();
        var inputs = NamedStruct.newBuilder().addNames("left").addNames("a").addNames("right").addNames("a")
                .setStruct(Type.Struct.newBuilder().addTypes(row).addTypes(row));
        assertThat(view.bindScalar(ref, inputs.build(), Set.of(), Optional.empty()).binding().output()).isEqualTo(row);
        inputs.setNames(3, "b");
        assertThatThrownBy(() -> view.bindScalar(ref, inputs.build(), Set.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("input contract mismatch");
    }

    private static FunctionCatalogView view() {
        return new FunctionCatalogView(CanonicalCapabilityInventoryTestFixture.inventory());
    }

    private static FunctionReferenceContract reference(FunctionCatalogView view, FunctionKind kind, String name) {
        return view.manifest().getDefinitionsList().stream().map(definition -> definition.getRef())
                .filter(ref -> ref.getKind() == kind && ref.getName().equals(name)).findFirst().orElseThrow();
    }

    private static NamedStruct arguments(Type... types) {
        var schema = NamedStruct.newBuilder();
        for (int ordinal = 0; ordinal < types.length; ordinal++) schema.addNames("argument_" + ordinal);
        return schema.setStruct(Type.Struct.newBuilder().addAllTypes(java.util.List.of(types))).build();
    }

    private static Type decimal(int precision, int scale) {
        return Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(precision).setScale(scale)
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static Type primitive(String kind) {
        var type = Type.newBuilder();
        var field = Type.getDescriptor().findFieldByName(kind);
        var value = type.newBuilderForField(field);
        value.setField(value.getDescriptorForType().findFieldByName("nullability"),
                Type.Nullability.NULLABILITY_REQUIRED.getValueDescriptor());
        return type.setField(field, value.build()).build();
    }
}
