package org.pangrova.client.jdbc;

import org.pangrova.client.http.PangrovaRestClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;

/**
 * JDBC DatabaseMetaData implementation for Pangrova.
 * Provides catalog/schema/table/column discovery for BI tools.
 *
 * <p>Result-set construction for discovery queries is delegated to
 * {@link MetadataResultSets} to keep this class readable.</p>
 */
public final class PangrovaDatabaseMetaData implements DatabaseMetaData {

    private static final String DATABASE_PRODUCT_NAME = "Pangrova";
    private static final String DATABASE_PRODUCT_VERSION = "0.1.0";
    private static final String DRIVER_NAME = "Pangrova JDBC Driver";
    private static final String DRIVER_VERSION = "0.1.0";

    private final PangrovaConnection connection;
    private final PangrovaRestClient restClient;

    PangrovaDatabaseMetaData(PangrovaConnection connection) {
        this.connection = connection;
        this.restClient = connection.restClient();
    }

    // --- Basic Info ---

    @Override public String getDatabaseProductName() throws SQLException { return DATABASE_PRODUCT_NAME; }
    @Override public String getDatabaseProductVersion() throws SQLException { return DATABASE_PRODUCT_VERSION; }
    @Override public String getDriverName() throws SQLException { return DRIVER_NAME; }
    @Override public String getDriverVersion() throws SQLException { return DRIVER_VERSION; }
    @Override public int getDriverMajorVersion() { return 0; }
    @Override public int getDriverMinorVersion() { return 1; }

    @Override
    public String getURL() throws SQLException {
        return "jdbc:pangrova://" + restClient.config().coordinatorHost() + ":"
                + restClient.config().coordinatorPort();
    }
    @Override public String getUserName() throws SQLException { return "pangrova-user"; }
    @Override public Connection getConnection() throws SQLException { return connection; }

    // --- Catalog Support ---

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return MetadataResultSets.catalogs(restClient);
    }
    @Override public boolean supportsCatalogsInDataManipulation() { return true; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return true; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public String getCatalogTerm() { return "catalog"; }

    // --- Schema Support ---

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return MetadataResultSets.schemas(restClient, catalog, schemaPattern);
    }
    @Override public boolean supportsSchemasInDataManipulation() { return true; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return true; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public String getSchemaTerm() { return "schema"; }

    // --- Table Support ---

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        return MetadataResultSets.tables(restClient, catalog, schemaPattern, tableNamePattern, types);
    }
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return MetadataResultSets.tableTypes();
    }

    // --- Column Support ---

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        return MetadataResultSets.columns(
                restClient, catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    // --- Feature Support ---

    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return true; }
    @Override public boolean storesMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return false; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return true; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return true; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return true; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public boolean isReadOnly() throws SQLException { return true; }
    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 0; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 0; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 0; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public boolean supportsTransactions() { return false; }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }

    // --- Empty Discovery Result Sets ---

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "REMARKS",
                        "PROCEDURE_TYPE", "SPECIFIC_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR));
    }
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                        "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
                        "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                        Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                        Types.SMALLINT, Types.INTEGER, Types.SMALLINT,
                        Types.VARCHAR, Types.VARCHAR));
    }
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR));
    }
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "Pangrova JDBC does not expose index metadata; use SHOW INDEXES");
    }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                             int[] types) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
                        "DATA_TYPE", "REMARKS", "BASE_TYPE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.SMALLINT));
    }
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return MetadataResultSets.typeInfo();
    }
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("getConnection with credentials not supported");
    }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
                        "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR));
    }
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR));
    }
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                        "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
                        "DEFERRABILITY"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT));
    }
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME",
                        "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
                        "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.INTEGER, Types.SMALLINT, Types.SMALLINT,
                        Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.SMALLINT, Types.INTEGER,
                        Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.SMALLINT));
    }
    @Override public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    @Override public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int getDatabaseMajorVersion() { return 0; }
    @Override public int getDatabaseMinorVersion() { return 1; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 2; }
    @Override public int getSQLStateType() { return DatabaseMetaData.sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return MetadataResultSets.empty(
                List.of("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
                List.of(Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR));
    }
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                        "FUNCTION_TYPE", "SPECIFIC_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR));
    }
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern)
            throws SQLException {
        return MetadataResultSets.empty(
                List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
                        "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
                        "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                        Types.VARCHAR, Types.INTEGER, Types.SMALLINT,
                        Types.VARCHAR, Types.VARCHAR));
    }
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                        "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                        "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.INTEGER, Types.VARCHAR));
    }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    // --- Null Sorting and Misc Capabilities ---

    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return true; }
    @Override public boolean supportsCorrelatedSubqueries() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }

    // --- Key and Privilege Discovery (unsupported, empty results) ---

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return foreignKeysResult();
    }
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return foreignKeysResult();
    }
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR",
                        "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR));
    }
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        return MetadataResultSets.empty(
                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE",
                        "PRIVILEGE", "IS_GRANTABLE"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR));
    }
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          boolean nullable) throws SQLException {
        return rowIdentifierResult();
    }
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return rowIdentifierResult();
    }
    @Override public long getMaxLogicalLobSize() { return 0; }
    @Override public boolean supportsRefCursors() { return false; }
    @Override public boolean supportsSharding() { return false; }

    // --- Helper Methods ---

    private static ResultSet foreignKeysResult() {
        return MetadataResultSets.empty(
                List.of("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                        "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                List.of(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.SMALLINT));
    }
    private static ResultSet rowIdentifierResult() {
        return MetadataResultSets.empty(
                List.of("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                        "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                List.of(Types.SMALLINT, Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                        Types.SMALLINT, Types.SMALLINT));
    }
}
