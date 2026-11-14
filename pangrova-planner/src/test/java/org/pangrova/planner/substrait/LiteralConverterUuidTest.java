package org.pangrova.planner.substrait;

import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.type.PlannerTypeFactory;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LiteralConverterUuidTest {
    private final PlannerTypeFactory types = new PlannerTypeFactory();
    private final RexBuilder rex = new RexBuilder(types);
    private final TypeMapper mapper = new TypeMapper();
    private final LiteralConverter literals = new LiteralConverter(mapper);

    @ParameterizedTest
    @ValueSource(strings = {"00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff", "00112233-4455-6677-8899-aabbccddeeff"})
    void uuidLiteralsRetainTheFull128Bits(String text) {
        UUID value = UUID.fromString(text);
        assertThat(literals.convert(rex.makeUuidLiteral(value)))
                .isEqualTo(ExpressionCreator.uuid(false, value));
    }

    @Test
    void nullUuidPreservesItsDeclaredType() {
        var nullable = types.createTypeWithNullability(types.createSqlType(SqlTypeName.UUID), true);
        assertThat(literals.convert(rex.makeNullLiteral(nullable)))
                .isEqualTo(ImmutableExpression.NullLiteral.builder().type(TypeCreator.NULLABLE.UUID).build());
        assertThat(mapper.toPangrovaParameterizedType(nullable).getDisplayName()).isEqualTo("UUID");
    }
}
