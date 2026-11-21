package org.pangrova.coordinator.generation;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.split.NativeReadSplitBinding;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Enumerates the exact canonical splits of one immutable source snapshot.
 *
 * <p>This is a control-plane component: it persists split descriptors only and
 * never reads source rows.  Its one-object/one-split invariant is required by
 * source-scoped artifact publication and by physical row-position locators.
 * Connectors that cannot preserve that identity fail closed.</p>
 */
public final class SnapshotGenerationSplitPlanner {

    public static final String EMPTY_SNAPSHOT = "pangrova:empty-snapshot";

    private static final int MAX_SPLITS = 1_000_000;
    private static final Duration ENUMERATION_TIMEOUT = Duration.ofMinutes(5);

    private final Function<String, CatalogHandle> catalogLookup;

    public SnapshotGenerationSplitPlanner(CatalogRegistry catalogRegistry) {
        this(Objects.requireNonNull(
                catalogRegistry, "catalogRegistry must not be null")::getCatalog);
    }

    SnapshotGenerationSplitPlanner(Function<String, CatalogHandle> catalogLookup) {
        this.catalogLookup = Objects.requireNonNull(
                catalogLookup, "catalogLookup must not be null");
    }

    /** Returns stable, checksummed v2 split descriptors for the requested snapshot. */
    public List<GenerationSplit> enumerate(
            QualifiedTableName table,
            String snapshotToken,
            ConnectorSession session) {
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(session, "session must not be null");
        if (snapshotToken == null || snapshotToken.isBlank()) {
            throw validation("Native artifact build requires an immutable snapshot token");
        }
        if (EMPTY_SNAPSHOT.equals(snapshotToken)) {
            return List.of();
        }
        long snapshotId = numericSnapshot(snapshotToken);
        CatalogHandle catalog = Objects.requireNonNull(
                catalogLookup.apply(table.catalog()),
                "catalog lookup returned null for " + table.catalog());
        ConnectorMetadata metadata = catalog.getMetadata(session);
        ConnectorTableHandle current = metadata.getTableHandle(
                session, new SchemaTableName(table.schema(), table.table()));
        if (current == null) {
            throw validation("Table disappeared while planning artifact build: " + table);
        }
        ConnectorTableHandle pinned = current.withTimeTravelSnapshot(snapshotId);
        if (pinned == current) {
            throw validation(
                    "Connector did not produce a snapshot-pinned table handle for " + table);
        }
        return drain(catalog, session, pinned, snapshotToken);
    }

    private static long numericSnapshot(String snapshotToken) {
        try {
            return Long.parseLong(snapshotToken);
        } catch (NumberFormatException failure) {
            throw validation(
                    "Native artifact build requires a numeric immutable snapshot token",
                    failure);
        }
    }

    private static List<GenerationSplit> drain(
            CatalogHandle catalog,
            ConnectorSession session,
            ConnectorTableHandle pinned,
            String snapshotToken) {
        ConnectorSplitSource source = catalog.getSplitManager().getSplits(
                ConnectorTransactionHandle.empty(),
                session,
                pinned,
                DynamicFilter.ALL,
                Constraint.EMPTY);
        List<GenerationSplit> result = new ArrayList<>();
        Set<String> objectIds = new HashSet<>();
        long deadline = System.nanoTime() + ENUMERATION_TIMEOUT.toNanos();
        try {
            while (!source.isFinished()) {
                if (System.nanoTime() > deadline) {
                    throw validation(
                            "Snapshot split enumeration exceeded " + ENUMERATION_TIMEOUT);
                }
                List<ConnectorSplit> batch = source.getNextBatch(1_000);
                if (batch.isEmpty() && !source.isFinished()) {
                    throw validation("Connector returned an empty non-terminal split batch");
                }
                for (ConnectorSplit split : batch) {
                    result.add(validateSplit(
                            NativeReadSplitBinding.bind(split, catalog, session), snapshotToken, objectIds));
                    if (result.size() > MAX_SPLITS) {
                        throw validation(
                                "Snapshot exceeds the durable split planning limit: "
                                        + MAX_SPLITS);
                    }
                }
            }
        } finally {
            source.close();
        }
        return List.copyOf(result);
    }

    private static GenerationSplit validateSplit(
            ConnectorSplit connectorSplit,
            String snapshotToken,
            Set<String> objectIds) {
        byte[] bytes = connectorSplit.toSplitInfoProto();
        if (bytes == null || bytes.length == 0) {
            throw validation("Connector split has no canonical v2 DataSplit encoding");
        }
        try {
            DataSplit split = DataSplit.parseFrom(bytes);
            if (!split.getUnknownFields().asMap().isEmpty()
                    || split.getSplitId().isBlank()
                    || split.getConnectorId().isBlank()
                    || split.getDataObjectId().isBlank()
                    || split.getOpaqueLocation().isBlank()
                    || !split.getSourceSnapshot().equals(snapshotToken)) {
                throw validation("Connector split identity does not match the pinned snapshot");
            }
            FileSplitDetail detail = DataSplitProtoCodec.fileDetail(split);
            if (detail.hasPaimon()
                    && (detail.getPaimon().getNeedsMerge()
                    || detail.getPaimon().getLsmLevelsCount() > 1)) {
                throw validation(
                        "Paimon MOR splits require a primary-key row locator; "
                                + "physical-position artifact publication is unsafe");
            }
            if (!objectIds.add(split.getDataObjectId())) {
                throw validation(
                        "One source object produced multiple build splits; "
                                + "segment identity must be explicit before distributed publication");
            }
            return GenerationSplit.of(split.getDataObjectId(), bytes);
        } catch (InvalidProtocolBufferException failure) {
            throw validation("Connector split is not a canonical v2 DataSplit", failure);
        }
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private static PangrovaException validation(String message, Throwable failure) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message, failure);
    }
}
