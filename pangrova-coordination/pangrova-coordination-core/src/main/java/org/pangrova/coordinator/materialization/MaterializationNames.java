package org.pangrova.coordinator.materialization;

import java.util.Locale;
import java.util.regex.Pattern;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.parser.api.ast.Identifier;

/** Resolves SQL identifiers to tenant-scoped materialization identities. */
final class MaterializationNames {

    private static final Pattern NAME_PART = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

    private MaterializationNames() {
    }

    static MaterializationId object(Identifier name, StatementExecutionContext context,
            String subject) {
        if (name.parts().size() != 3
                || name.parts().stream().anyMatch(part -> !NAME_PART.matcher(part).matches())) {
            throw new IllegalArgumentException(
                    subject + " names must be unquoted catalog.schema.table identifiers");
        }
        String qualified = String.join(".", name.parts()).toLowerCase(Locale.ROOT);
        return new MaterializationId(MaterializationKernelFactory.tenant(context),
                QualifiedName.parse(qualified));
    }
}
