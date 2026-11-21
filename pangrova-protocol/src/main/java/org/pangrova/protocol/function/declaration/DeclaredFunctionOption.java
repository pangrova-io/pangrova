package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import io.substrait.proto.FunctionOption;

import java.util.Set;

/** An executable option from the sole declaration, not a function-name side table. */
public record DeclaredFunctionOption(String name, String behavior, String enabledValue, String disabledValue) {
    public DeclaredFunctionOption {
        if (name == null || name.isBlank() || !"NULL_TREATMENT".equals(behavior)
                || enabledValue == null || enabledValue.isBlank() || disabledValue == null
                || disabledValue.isBlank() || enabledValue.equals(disabledValue)) {
            throw new IllegalArgumentException("Malformed executable function option");
        }
    }

    static DeclaredFunctionOption parse(JsonNode option) {
        BindingDeclarationJson.keys(option, Set.of("name", "behavior", "enabledValue", "disabledValue"), Set.of());
        return new DeclaredFunctionOption(BindingDeclarationJson.text(option, "name"),
                BindingDeclarationJson.text(option, "behavior"), BindingDeclarationJson.text(option, "enabledValue"),
                BindingDeclarationJson.text(option, "disabledValue"));
    }

    public FunctionOption select(boolean enabled) {
        return FunctionOption.newBuilder().setName(name).addPreference(enabled ? enabledValue : disabledValue).build();
    }
}
