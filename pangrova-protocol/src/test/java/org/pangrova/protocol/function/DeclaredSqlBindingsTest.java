package org.pangrova.protocol.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeclaredSqlBindingsTest {
    @Test
    void namesAliasesAndStandardOperatorsResolveFromTheSoleDeclaration() {
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, "ceiling", ""))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_SCALAR, "arithmetic", "ceil"));
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_AGGREGATE, "", "VAR_POP"))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_AGGREGATE, "aggregate", "variance_pop"));
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_WINDOW, "$sum0", ""))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_WINDOW, "window", "sum0"));
    }

    @Test
    void declaredPresenceDoesNotInventAnotherContextOrExecutionAvailability() {
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, "ROW_NUMBER", ""))
                .isEmpty();
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_WINDOW, "unknown_function", ""))
                .isEmpty();
        assertThatThrownBy(() -> FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_UNSPECIFIED, "SUM", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void caseInsensitiveDuplicateNamesFailInsteadOfChoosingDeclarationOrder() throws Exception {
        var json = new ObjectMapper();
        var first = json.readTree("""
                {"kind":"SCALAR","namespace":"one","name":"first","sqlNames":["ROUND"]}
                """);
        var second = json.readTree("""
                {"kind":"SCALAR","namespace":"two","name":"second","sqlNames":["round"]}
                """);
        assertThatThrownBy(() -> new DeclaredSqlBindings(List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ambiguous SQL function binding");
    }
}
