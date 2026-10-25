package org.pangrova.coordinator.query.statement.dml;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.ValidationException;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagedFileWriteGuardTest {

    private static final List<String> COLUMNS = List.of("id", "img");

    @Test
    void nonNullValueInManagedColumnIsRejected() {
        assertThatThrownBy(() -> ManagedFileWriteGuard.requireSupported(
                table(Map.of("pangrova.file.column.img", "true",
                        "pangrova.file.storage.img", "MANAGED")),
                COLUMNS, List.of(fileRow())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("FILE MANAGED column 'img'");
    }

    @Test
    void nullValueInManagedColumnIsAccepted() {
        assertThatCode(() -> ManagedFileWriteGuard.requireSupported(
                table(Map.of("pangrova.file.storage.img", "MANAGED")),
                COLUMNS, List.of(List.of(Literal.ofInteger(1L), Literal.ofNull()))))
                .doesNotThrowAnyException();
    }

    @Test
    void externalColumnsAreUnaffected() {
        assertThatCode(() -> ManagedFileWriteGuard.requireSupported(
                table(Map.of("pangrova.file.column.img", "true")),
                COLUMNS, List.of(fileRow())))
                .doesNotThrowAnyException();
    }

    private static ConnectorTableMetadata table(Map<String, String> properties) {
        return new ConnectorTableMetadata(new SchemaTableName("s", "t"), COLUMNS, properties);
    }

    private static List<Expression> fileRow() {
        return List.of(
                Literal.ofInteger(1L),
                new FunctionCall(Identifier.of("FILE_FROM_URL"),
                        List.<Expression>of(Literal.ofString("s3://b/photo.jpg"))));
    }
}
