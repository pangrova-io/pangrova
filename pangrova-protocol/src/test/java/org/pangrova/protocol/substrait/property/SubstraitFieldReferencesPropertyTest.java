package org.pangrova.protocol.substrait.property;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubstraitFieldReferencesPropertyTest {
    @Property(tries = 150, seed = "832672597106")
    void ordinalAndExplicitRootSurviveWireRoundTripWithoutImplicitOrigins(
            @ForAll @IntRange(min = 1, max = 32) int width,
            @ForAll int selector, @ForAll boolean nullable) throws Exception {
        var type = Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(38).setScale(18)
                .setNullability(nullable ? Type.Nullability.NULLABILITY_NULLABLE
                        : Type.Nullability.NULLABILITY_REQUIRED).setTypeVariationReference(17)).build();
        var schema = NamedStruct.newBuilder();
        for (int ordinal = 0; ordinal < width; ordinal++) {
            schema.addNames("field_" + ordinal).getStructBuilder().addTypes(type);
        }
        int ordinal = Math.floorMod(selector, width);
        var value = Expression.parseFrom(SubstraitRelationLayout.rootFieldReference(ordinal).toByteArray());
        assertThat(value.getSelection().hasRootReference()).isTrue();
        assertThat(value.getSelection().getDirectReference().getStructField().getField()).isEqualTo(ordinal);
        assertThat(SubstraitRelationLayout.typeOf(value, schema.build())).isEqualTo(type);
        var missing = value.toBuilder().setSelection(value.getSelection().toBuilder().clearRootReference()).build();
        assertThatThrownBy(() -> SubstraitRelationLayout.typeOf(missing, schema.build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit supported root");
    }
}
