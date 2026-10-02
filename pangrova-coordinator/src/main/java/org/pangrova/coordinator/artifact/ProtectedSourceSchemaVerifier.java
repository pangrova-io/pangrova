package org.pangrova.coordinator.artifact;

import io.substrait.proto.NamedStruct;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.Objects;

/** Verifies exact historical source schema against the server-compiled scan, without reading rows. */
public final class ProtectedSourceSchemaVerifier {
    private final CatalogRegistry catalogRegistry;

    public ProtectedSourceSchemaVerifier(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = Objects.requireNonNull(catalogRegistry, "catalogRegistry must not be null");
    }

    public void verify(QualifiedTableName source, String snapshot, ConnectorSession session,
                AuthorizedRelation compiled) {
        if (!source.catalog().equals(session.getCatalog().orElse(null))
                || !source.schema().equals(session.getSchema().orElse(null))) {
            throw invalid("Protected source connector scope differs from the compiled source");
        }
        long version = exactSnapshot(snapshot);
        byte[] expected = compiled.sourceSchemas().get(source.toString());
        if (expected == null || expected.length == 0 || compiled.sourceSchemas().size() != 1) {
            throw invalid("Authenticated compiler did not provide one exact source scan schema");
        }
        var metadata = catalogRegistry.getCatalog(source.catalog()).getMetadata(session);
        var current = metadata.getTableHandle(session, new SchemaTableName(source.schema(), source.table()));
        if (current == null) {
            throw invalid("Protected source table no longer exists");
        }
        var pinned = current.withTimeTravelSnapshot(version);
        if (pinned == current) {
            throw invalid("Protected source connector did not bind the exact historical snapshot");
        }
        var historical = metadata.getTableMetadata(session, pinned);
        var currentSchema = metadata.getTableMetadata(session, current);
        if (!historical.table().equals(new SchemaTableName(source.schema(), source.table()))
                || historical.typedColumns().isEmpty()
                || historical.columns().size() != historical.typedColumns().size()
                || !historical.typedColumns().equals(currentSchema.typedColumns())) {
            throw invalid("Protected source historical schema is absent or differs from its table identity");
        }
        try {
            NamedStruct input = NamedStruct.parseFrom(expected);
            NamedStruct actual = NativeDatasetSchemaCompiler.compile(historical.typedColumns()).schema();
            if (!input.equals(actual)) {
                throw invalid("Protected source historical schema differs from the authenticated compiled scan; "
                        + "rebuild a definition against a compatible snapshot");
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw invalid("Authenticated compiler returned an invalid source schema");
        }
    }

    private static long exactSnapshot(String snapshot) {
        try {
            long version = Long.parseLong(snapshot);
            if (version <= 0 || !Long.toString(version).equals(snapshot)) {
                throw new NumberFormatException("not a canonical positive snapshot");
            }
            return version;
        } catch (NumberFormatException failure) {
            throw invalid("Protected materialization requires one exact positive numeric source snapshot");
        }
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
