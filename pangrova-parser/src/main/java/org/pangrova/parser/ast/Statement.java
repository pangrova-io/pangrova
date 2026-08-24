package org.pangrova.parser.ast;

import java.util.Map;
import java.util.Optional;

import org.pangrova.parser.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.BackfillColumnStatement;
import org.pangrova.parser.ast.statement.CancelBackfillStatement;
import org.pangrova.parser.ast.statement.CreateBranchStatement;
import org.pangrova.parser.ast.statement.CreateEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.CreateIndexStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.CreateTagStatement;
import org.pangrova.parser.ast.statement.CreateViewStatement;
import org.pangrova.parser.ast.statement.DescribeCatalogStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropBranchStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.DropIndexStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.DropTagStatement;
import org.pangrova.parser.ast.statement.DropViewStatement;
import org.pangrova.parser.ast.statement.ExplainStatement;
import org.pangrova.parser.ast.statement.GrantTenantStatement;
import org.pangrova.parser.ast.statement.IncrementalReadStatement;
import org.pangrova.parser.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.ast.statement.RevokeTenantStatement;
import org.pangrova.parser.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.ast.statement.ShowBackfillStatusStatement;
import org.pangrova.parser.ast.statement.ShowBranchesStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.parser.ast.statement.ShowEmbeddingModelsStatement;
import org.pangrova.parser.ast.statement.ShowIndexesStatement;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.ShowTagsStatement;
import org.pangrova.parser.ast.statement.ShowViewsStatement;
import org.pangrova.parser.ast.statement.UseBranchStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
import org.pangrova.parser.ast.statement.stream.ShowStreamsStatement;

/**
 * Base interface for all SQL statement AST nodes.
 *
 * <p>The {@link #type()} method enables exhaustive switch expressions with
 * compile-time completeness checking via {@link StatementType}. Adding a new
 * statement type requires adding a corresponding enum value to {@link StatementType}
 * and an entry to the type registry; any switch expression that does not cover
 * the new value will fail to compile.</p>
 */
public interface Statement {

    /**
     * Returns the source location of this statement in the original SQL text.
     * Default returns empty; populated via {@link SourceLocationRegistry} during parsing.
     *
     * @return source location if available, empty otherwise
     */
    default Optional<SourceLocation> sourceLocation() {
        return Optional.empty();
    }

    /**
     * Returns the concrete type of this statement for exhaustive switch dispatch.
     *
     * @throws IllegalStateException if the statement type is not recognized
     */
    default StatementType type() {
        StatementType resolved = StatementTypeRegistry.resolve(this);
        if (resolved != null) {
            return resolved;
        }
        throw new IllegalStateException("Unknown statement type: " + getClass().getName());
    }
}

/**
 * Internal registry mapping concrete statement classes to their {@link StatementType}.
 * Package-private to keep the lookup centralized while avoiding a 60+ line method.
 */
final class StatementTypeRegistry {

    private static final Map<Class<? extends Statement>, StatementType> TYPE_MAP = Map.ofEntries(
            Map.entry(Query.class, StatementType.QUERY),
            Map.entry(SetQuery.class, StatementType.SET_QUERY),
            Map.entry(CteQuery.class, StatementType.CTE_QUERY),
            Map.entry(InsertStatement.class, StatementType.INSERT),
            Map.entry(CreateCatalogStatement.class, StatementType.CREATE_CATALOG),
            Map.entry(CreateContinuousQueryStatement.class, StatementType.CREATE_CONTINUOUS_QUERY),
            Map.entry(CreateDynamicTableStatement.class, StatementType.CREATE_DYNAMIC_TABLE),
            Map.entry(ShowSchemasStatement.class, StatementType.SHOW_SCHEMAS),
            Map.entry(AlterCatalogStatement.class, StatementType.ALTER_CATALOG),
            Map.entry(AlterDynamicTableStatement.class, StatementType.ALTER_DYNAMIC_TABLE),
            Map.entry(AlterEmbeddingModelStatement.class, StatementType.ALTER_EMBEDDING_MODEL),
            Map.entry(AlterIndexStatement.class, StatementType.ALTER_INDEX),
            Map.entry(BackfillColumnStatement.class, StatementType.BACKFILL_COLUMN),
            Map.entry(CancelBackfillStatement.class, StatementType.CANCEL_BACKFILL),
            Map.entry(CreateBranchStatement.class, StatementType.CREATE_BRANCH),
            Map.entry(CreateEmbeddingModelStatement.class, StatementType.CREATE_EMBEDDING_MODEL),
            Map.entry(CreateIndexStatement.class, StatementType.CREATE_INDEX),
            Map.entry(CreateSchemaStatement.class, StatementType.CREATE_SCHEMA),
            Map.entry(CreateTableStatement.class, StatementType.CREATE_TABLE),
                Map.entry(
                    org.pangrova.parser.ast.statement.CreateTableAsSelectStatement.class,
                    StatementType.CREATE_TABLE_AS_SELECT),
            Map.entry(CreateTagStatement.class, StatementType.CREATE_TAG),
            Map.entry(CreateViewStatement.class, StatementType.CREATE_VIEW),
            Map.entry(DescribeCatalogStatement.class, StatementType.DESCRIBE_CATALOG),
            Map.entry(DescribeDynamicTableStatement.class, StatementType.DESCRIBE_DYNAMIC_TABLE),
            Map.entry(DescribeTableStatement.class, StatementType.DESCRIBE_TABLE),
            Map.entry(DropBranchStatement.class, StatementType.DROP_BRANCH),
            Map.entry(DropCatalogStatement.class, StatementType.DROP_CATALOG),
            Map.entry(DropDynamicTableStatement.class, StatementType.DROP_DYNAMIC_TABLE),
            Map.entry(DropEmbeddingModelStatement.class, StatementType.DROP_EMBEDDING_MODEL),
            Map.entry(DropIndexStatement.class, StatementType.DROP_INDEX),
            Map.entry(DropTableStatement.class, StatementType.DROP_TABLE),
            Map.entry(DropTagStatement.class, StatementType.DROP_TAG),
            Map.entry(DropViewStatement.class, StatementType.DROP_VIEW),
            Map.entry(ExplainStatement.class, StatementType.EXPLAIN),
            Map.entry(GrantTenantStatement.class, StatementType.GRANT_TENANT),
            Map.entry(IncrementalReadStatement.class, StatementType.INCREMENTAL_READ),
            Map.entry(ReclusterTableStatement.class, StatementType.RECLUSTER_TABLE),
            Map.entry(RevokeTenantStatement.class, StatementType.REVOKE_TENANT),
            Map.entry(SetClusteringOrderStatement.class, StatementType.SET_CLUSTERING_ORDER),
            Map.entry(ShowBackfillStatusStatement.class, StatementType.SHOW_BACKFILL_STATUS),
            Map.entry(ShowBranchesStatement.class, StatementType.SHOW_BRANCHES),
            Map.entry(ShowCatalogsStatement.class, StatementType.SHOW_CATALOGS),
            Map.entry(ShowClusteringStatusStatement.class, StatementType.SHOW_CLUSTERING_STATUS),
            Map.entry(ShowDynamicTablesStatement.class, StatementType.SHOW_DYNAMIC_TABLES),
            Map.entry(ShowEmbeddingModelsStatement.class, StatementType.SHOW_EMBEDDING_MODELS),
            Map.entry(ShowIndexesStatement.class, StatementType.SHOW_INDEXES),
            Map.entry(ShowTablesStatement.class, StatementType.SHOW_TABLES),
            Map.entry(ShowTagsStatement.class, StatementType.SHOW_TAGS),
            Map.entry(ShowViewsStatement.class, StatementType.SHOW_VIEWS),
            Map.entry(UseBranchStatement.class, StatementType.USE_BRANCH),
            Map.entry(DeleteStatement.class, StatementType.DELETE),
            Map.entry(InsertOverwriteStatement.class, StatementType.INSERT_OVERWRITE),
            Map.entry(InsertSelectStatement.class, StatementType.INSERT_SELECT),
            Map.entry(MergeStatement.class, StatementType.MERGE),
            Map.entry(UpdateStatement.class, StatementType.UPDATE),
            Map.entry(CallStatement.class, StatementType.CALL),
            Map.entry(CompactStatement.class, StatementType.COMPACT),
            Map.entry(OptimizeStatement.class, StatementType.OPTIMIZE),
            Map.entry(VacuumStatement.class, StatementType.VACUUM),
            Map.entry(AlterStreamStatement.class, StatementType.ALTER_STREAM),
            Map.entry(CreateStreamStatement.class, StatementType.CREATE_STREAM),
            Map.entry(DescribeStreamStatement.class, StatementType.DESCRIBE_STREAM),
            Map.entry(DropStreamStatement.class, StatementType.DROP_STREAM),
            Map.entry(ShowStreamsStatement.class, StatementType.SHOW_STREAMS),
            Map.entry(SetSessionStatement.class, StatementType.SET_SESSION)
    );

    private StatementTypeRegistry() {}

    static StatementType resolve(Statement statement) {
        StatementType type = TYPE_MAP.get(statement.getClass());
        if (type != null) {
            return type;
        }
        // Handle sealed sub-hierarchies (AlterTableStatement, ShowMetadataStatement)
        if (statement instanceof AlterTableStatement) return StatementType.ALTER_TABLE;
        if (statement instanceof ShowMetadataStatement) return StatementType.SHOW_METADATA;
        return null;
    }
}
