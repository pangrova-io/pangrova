package org.pangrova.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.pangrova.protocol.v2.CapabilityInventory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Serialization consults actual Worker inventory; defaults are not runtime admission. */
class RuntimeCapabilityValidationTest {
    private static final String QUERY = "SELECT EXTRACT(YEAR FROM order_date) FROM orders";

    @Test
    void actualYearCapabilitySurvivesValidatedSerializationAndInventoryChanges() throws Exception {
        CapabilityInventory actual = CapabilityInventory.parseFrom(Files.readAllBytes(Path.of(
                "../pangrova-protocol/src/test/resources/function-contracts/actual-native-catalogue.bin")));
        var advertised = NativeCapabilityRegistry.fromInventories(List.of(actual));
        assertThat(advertised.isFunctionSupported("year")).isTrue();
        assertThat(advertised.isFunctionSupported("nonexistent_function")).isFalse();
        var current = new AtomicReference<>(advertised);
        var compiler = compiler().withNativeCapabilities(current::get);

        assertThat(compiler.compile(QUERY)).isNotEmpty();
        // The same compiler must not cache an earlier permissive snapshot.
        // Standalone defaults also advertise year, so remove only that function
        // from an otherwise equivalent Worker inventory.
        var withoutYear = NativeCapabilityRegistry.defaults();
        withoutYear.update(Set.copyOf(actual.getSupportedSubstraitRelTypesList()),
                Set.copyOf(actual.getSupportedSubstraitExpressionTypesList()),
                actual.getFunctionsList().stream().map(function -> function.getRef().getName())
                        .filter(name -> !name.equals("year")).collect(Collectors.toSet()));
        current.set(withoutYear);
        assertThatThrownBy(() -> compiler.compile(QUERY)).isInstanceOf(PlannerException.class)
                .hasMessageContaining("year");
        current.set(advertised);
        assertThat(compiler.compile(QUERY)).isNotEmpty();
    }

    @Test
    void missingOrMalformedInventoriesCannotAcquireDefaultCapabilities() {
        assertThatThrownBy(() -> NativeCapabilityRegistry.fromInventories(List.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("active Worker inventory");
        assertThatThrownBy(() -> NativeCapabilityRegistry.fromInventories(
                List.of(CapabilityInventory.getDefaultInstance()))).isInstanceOf(IllegalArgumentException.class);
    }

    private static SqlCompiler compiler() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory types) {
                return types.builder().add("order_date", SqlTypeName.DATE).build();
            }
        });
        return new SqlCompiler(new PangrovaPlanner(schema), new SubstraitConverter(),
                new SubstraitPlanSerializer());
    }
}
