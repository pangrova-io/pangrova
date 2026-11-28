package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeclaredSqlBindingIndexTest {
    @Test
    void namesAliasesAndStandardOperatorsResolveFromTheSoleDeclaration() {
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, "ceiling", ""))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_SCALAR, "arithmetic", "ceil",
                        "extension:io.substrait:functions_arithmetic"));
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_AGGREGATE, "", "VAR_POP"))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_AGGREGATE, "aggregate", "variance_pop",
                        "extension:io.substrait:functions_aggregate_generic"));
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_WINDOW, "$sum0", ""))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_WINDOW, "window", "sum0",
                        "extension:org.pangrova:functions_window"));
        assertThat(FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, "variant_get", ""))
                .contains(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_SCALAR, "variant", "variant_get",
                        "extension:org.pangrova:functions_variant"));
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
                {"kind":"SCALAR","namespace":"one","canonicalUrn":"extension:org.pangrova:functions_one","name":"first","sqlNames":["ROUND"]}
                """);
        var second = json.readTree("""
                {"kind":"SCALAR","namespace":"two","canonicalUrn":"extension:org.pangrova:functions_two","name":"second","sqlNames":["round"]}
                """);
        assertThatThrownBy(() -> new DeclaredSqlBindingIndex(List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ambiguous SQL function binding");
    }

    @Test
    void extensionIdentityRequiresAnExplicitDeclarationInsteadOfNamespaceFallback() throws Exception {
        assertThat(FunctionBindingDeclarations.canonicalUrn(FunctionKind.FUNCTION_KIND_SCALAR, "datetime", "year"))
                .isEqualTo("extension:org.pangrova:functions_datetime");
        assertThatThrownBy(() -> FunctionBindingDeclarations.canonicalUrn(
                FunctionKind.FUNCTION_KIND_SCALAR, "datetime", "invented"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown function binding");
        var json = new ObjectMapper();
        for (String urn : new String[]{"", "http://invented", "extension:io.substrait:functions_string\n"}) {
            var declaration = json.createObjectNode().put("namespace", "string").put("name", "trim")
                    .put("implementation", "trim").put("kind", "SCALAR").put("canonicalUrn", urn);
            assertThatThrownBy(() -> BindingDeclarationValidation.binding(declaration))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var missing = json.createObjectNode().put("namespace", "string").put("name", "trim")
                .put("implementation", "trim").put("kind", "SCALAR");
        assertThatThrownBy(() -> BindingDeclarationValidation.binding(missing)).isInstanceOf(IllegalArgumentException.class);
    }
}
