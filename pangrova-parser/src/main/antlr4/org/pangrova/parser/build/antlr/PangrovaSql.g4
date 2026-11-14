grammar PangrovaSql;

// ============================================================
// Parser Rules
// ============================================================

singleStatement
    : statement SEMICOLON? EOF
    ;

statement
    : explainStatement               #explainStmt
    | cteQuery                       #cteQueryStatement
    | setQuery                       #queryStatement
    | incrementalReadStatement       #incrementalReadStmt
    | compactTable                   #compactTableStatement
    | optimizeTable                  #optimizeTableStatement
    | vacuumTable                    #vacuumTableStatement
    | analyzeTable                   #analyzeTableStatement
    | createCatalog                  #createCatalogStatement
    | dropCatalog                    #dropCatalogStatement
    | showCatalogs                   #showCatalogsStatement
    | showSchemas                    #showSchemasStatement
    | showTables                     #showTablesStatement
    | showTableControls              #showTableControlsStatement
    | showViews                      #showViewsStatement
    | showSemanticViews              #showSemanticViewsStatement
    | showSnapshots                  #showSnapshotsStatement
    | showFiles                      #showFilesStatement
    | showManifests                  #showManifestsStatement
    | showPartitions                 #showPartitionsStatement
    | describeCatalog                #describeCatalogStatement
    | describeTable                  #describeTableStatement
    | alterCatalog                   #alterCatalogStatement
    | alterTable                     #alterTableStatement
    | callStatement                  #callProcedureStatement
    | createTable                    #createTableStatement
    | createView                     #createViewStatement
    | createSemanticView             #createSemanticViewStatement
    | publishSemanticView            #publishSemanticViewStatement
    | createFeatureView              #createFeatureViewStatement
    | publishFeatureView             #publishFeatureViewStatement
    | materializeFeatureView         #materializeFeatureViewStatement
    | dropView                       #dropViewStatement
    | createSchema                   #createSchemaStatement
    | dropTable                      #dropTableStatement
    | insertInto                     #insertIntoStatement
    | insertOverwrite                #insertOverwriteStatement
    | insertSelect                   #insertSelectStatement
    | deleteFrom                     #deleteFromStatement
    | updateStatement                #updateStmt
    | mergeInto                      #mergeIntoStatement
    | createDynamicTable             #createDynamicTableStatement
    | alterDynamicTable              #alterDynamicTableStatement
    | dropDynamicTable               #dropDynamicTableStatement
    | showDynamicTables              #showDynamicTablesStatement
    | describeDynamicTable           #describeDynamicTableStatement
    | createMaterializedView         #createMaterializedViewStatement
    | alterMaterializedView          #alterMaterializedViewStatement
    | refreshMaterializedView        #refreshMaterializedViewStatement
    | dropMaterializedView           #dropMaterializedViewStatement
    | showMaterializedViews          #showMaterializedViewsStatement
    | describeMaterializedView       #describeMaterializedViewStatement
    | showMaterializations           #showMaterializationsStatement
    | showMaterializationRefreshHistory #showMaterializationRefreshHistoryStatement
    | showMaterializationDependencies #showMaterializationDependenciesStatement
    | showMaterializationGcHistory   #showMaterializationGcHistoryStatement
    | createIndex                    #createIndexStatement
    | dropIndex                      #dropIndexStatement
    | showIndexes                    #showIndexesStatement
    | alterIndex                     #alterIndexStatement
    | setClusteringOrder             #setClusteringOrderStatement
    | reclusterTable                 #reclusterTableStatement
    | showClusteringStatus           #showClusteringStatusStatement
    | createStream                   #createStreamStatement
    | alterStream                    #alterStreamStatement
    | dropStream                     #dropStreamStatement
    | showStreams                     #showStreamsStatement
    | describeStream                 #describeStreamStatement
    | createBranch                   #createBranchStatement
    | dropBranch                     #dropBranchStatement
    | createTag                      #createTagStatement
    | dropTag                        #dropTagStatement
    | useBranch                      #useBranchStatement
    | showBranches                   #showBranchesStatement
    | showTags                       #showTagsStatement
    | createEmbeddingModel           #createEmbeddingModelStatement
    | alterEmbeddingModel            #alterEmbeddingModelStatement
    | dropEmbeddingModel             #dropEmbeddingModelStatement
    | showEmbeddingModels            #showEmbeddingModelsStatement
    | backfillColumn                 #backfillColumnStatement
    | cancelBackfill                 #cancelBackfillStatement
    | showBackfillStatus             #showBackfillStatusStatement
    | createStorageProfile           #createStorageProfileStatement
    | dropStorageProfile             #dropStorageProfileStatement
    | showStorageProfiles            #showStorageProfilesStatement
    | createVolume                   #createVolumeStatement
    | alterVolume                    #alterVolumeStatement
    | dropVolume                     #dropVolumeStatement
    | showVolumes                    #showVolumesStatement
    | describeVolume                 #describeVolumeStatement
    | createStage                    #createStageStatement
    | alterStage                     #alterStageStatement
    | dropStage                      #dropStageStatement
    | showStages                     #showStagesStatement
    | describeStage                  #describeStageStatement
    | listStage                      #listStageStatement
    | removeStage                    #removeStageStatement
    | copyInto                       #copyIntoStatement
    | showPrivilegeMutation          #showPrivilegeMutationStatement
    | grantTenant                    #grantTenantStatement
    | revokeTenant                   #revokeTenantStatement
    | grantPrivilege                 #grantPrivilegeStatement
    | revokePrivilege                #revokePrivilegeStatement
    | setSession                     #setSessionStatement
    ;

// --- Privilege administration ---
grantTenant : GRANT TENANT identifier TO identifier;
revokeTenant : REVOKE TENANT identifier FROM identifier;

showPrivilegeMutation
    : SHOW PRIVILEGE MUTATION STRING_LITERAL
    ;

grantPrivilege
    : GRANT privilegeName (COMMA privilegeName)* ON privilegeObjectType qualifiedName
      TO ROLE? identifier (COMMA identifier)* (WITH GRANT OPTION)?
    ;

revokePrivilege
    : REVOKE (GRANT OPTION FOR)? privilegeName (COMMA privilegeName)* ON privilegeObjectType qualifiedName
      FROM ROLE? identifier (COMMA identifier)* (CASCADE | RESTRICT)?
    ;

privilegeName
    : SELECT | ALL PRIVILEGES? | CREATE (TABLE | VIEW | SCHEMA | CATALOG)
    | DROP (SCHEMA | CATALOG)? | ALTER (SCHEMA | CATALOG)? | identifier
    ;

privilegeObjectType
    : TABLE | SCHEMA | CATALOG | VOLUME | STAGE | STORAGE PROFILE | STORAGE_PROFILE
    ;

// --- Incremental Read ---

incrementalReadStatement
    : SELECT selectItems FROM qualifiedName
      CHANGES BETWEEN SNAPSHOT fromSnapshot=expression AND toSnapshot=expression
      (WHERE where=expression)?
      (ORDER BY orderByList)?
      (LIMIT limit=expression)?
    ;

// --- Standard SQL: SELECT query ---

query
    : SELECT distinct=DISTINCT? selectItems
      (FROM tableRefList)?
      (WHERE where=expression)?
      (GROUP BY groupByClause)?
      (HAVING having=expression)?
      (QUALIFY qualify=expression)?
      (WINDOW windowDefinitionList)?
      (ORDER BY orderByList)?
      (RERANK BY rerankClause)?
      (LIMIT limit=expression)?
      (OFFSET offset=expression)?
    ;

windowDefinitionList
    : windowDefinition (COMMA windowDefinition)*
    ;

windowDefinition
    : name=identifier AS LPAREN
        (PARTITION BY expressionList)?
        (ORDER BY orderByList)?
        windowFrame?
      RPAREN
    ;

setQuery
    : query (setOperator query)*
    ;

setOperator
    : UNION ALL       #unionAll
    | UNION           #unionDistinct
    | INTERSECT       #intersectOp
    | EXCEPT          #exceptOp
    ;

cteQuery
    : WITH RECURSIVE? cteDefinition (COMMA cteDefinition)* setQuery
    ;

cteDefinition
    : identifier (LPAREN columnNameList RPAREN)? AS LPAREN setQuery RPAREN
      cteSearchClause?
      cteCycleClause?
    ;

cteSearchClause
    : SEARCH (DEPTH | BREADTH) FIRST_KW BY orderByList SET identifier
    ;

cteCycleClause
    : CYCLE columnNameList SET identifier TO literal DEFAULT literal USING identifier
    ;

selectItems
    : selectItem (COMMA selectItem)*
    ;

selectItem
    : STAR                                          #selectAll
    | expression (AS? alias=identifier)?            #selectExpression
    ;

tableRefList
    : joinedTable (COMMA joinedTable)*
    ;

joinedTable
    : tableRef (joinType tableRef joinCondition?)*
    ;

joinCondition
    : ON expression                                     #joinOnCondition
    | USING LPAREN columnNameList RPAREN                #joinUsingCondition
    ;

joinType
    : INNER JOIN
    | LEFT OUTER? JOIN
    | RIGHT OUTER? JOIN
    | FULL OUTER? JOIN
    | CROSS JOIN
    | JOIN
    ;

tableRef
    : qualifiedName timeTravelSpec? tableSampleClause? (AS? alias=identifier)?       #namedTableRef
    | LPAREN setQuery RPAREN (AS? alias=identifier)?              #derivedTableRef
    | LPAREN VALUES valuesList RPAREN (AS? alias=identifier (LPAREN columnNameList RPAREN)?)? #valuesTableRef
    | LATERAL TABLE LPAREN tableFunctionCall RPAREN
        (AS? alias=identifier LPAREN columnNameList RPAREN)?      #lateralTableFunctionRef
    | TABLE LPAREN tableFunctionCall RPAREN
        (AS? alias=identifier (LPAREN columnNameList RPAREN)?)?   #standaloneTableFunctionRef
    | GENERATE_SERIES LPAREN rangeStart=expression COMMA rangeEnd=expression
        (COMMA rangeStep=expression)? RPAREN
        (AS? alias=identifier (LPAREN columnNameList RPAREN)?)?   #directGenerateSeriesRef
    | LATERAL EXPLODE LPAREN expression RPAREN
        (AS? alias=identifier LPAREN columnNameList RPAREN)?        #lateralExplodeRef
    | LATERAL POSEXPLODE LPAREN expression RPAREN
        (AS? alias=identifier LPAREN columnNameList RPAREN)?        #lateralPosexplodeRef
    | LATERAL? UNNEST LPAREN expression RPAREN (WITH ORDINALITY)?
        (AS? alias=identifier (LPAREN columnNameList RPAREN)?)?      #unnestTableRef
    | LATERAL LPAREN setQuery RPAREN (AS? alias=identifier)?      #lateralDerivedTableRef
    | tableRef PIVOT LPAREN pivotAggregateList FOR identifier IN LPAREN pivotValueList RPAREN RPAREN (AS? alias=identifier)?   #pivotTableRef
    | tableRef UNPIVOT (nullsOption)? LPAREN identifier FOR identifier IN LPAREN unpivotColumnList RPAREN RPAREN (AS? alias=identifier)?  #unpivotTableRef
    ;

tableSampleClause
    : TABLESAMPLE sampleMethod LPAREN percentage=expression RPAREN
      (REPEATABLE LPAREN seed=expression RPAREN)?
    ;

sampleMethod
    : BERNOULLI
    | SYSTEM_KW
    ;

nullsOption
    : INCLUDE NULLS
    | EXCLUDE NULLS
    ;

pivotAggregateList
    : pivotAggregate (COMMA pivotAggregate)*
    ;

pivotAggregate
    : qualifiedName LPAREN expression RPAREN (AS? alias=identifier)?
    ;

pivotValueList
    : pivotValue (COMMA pivotValue)*
    ;

pivotValue
    : expression (AS? alias=identifier)?
    ;

unpivotColumnList
    : unpivotColumn (COMMA unpivotColumn)*
    ;

unpivotColumn
    : identifier (AS? alias=STRING_LITERAL)?
    ;

tableFunctionCall
    : EXPLODE LPAREN expression RPAREN
    | POSEXPLODE LPAREN expression RPAREN
    | GENERATE_SERIES LPAREN rangeStart=expression COMMA rangeEnd=expression
        (COMMA rangeStep=expression)? RPAREN
    ;

timeTravelSpec
    : FOR SYSTEM_TIME AS OF expression              #systemTimeTravel
    | FOR SYSTEM_VERSION AS OF expression           #systemVersionTravel
    | FOR VERSION AS OF expression                  #systemVersionTravel
    | VERSION AS OF BRANCH STRING_LITERAL           #branchVersionTravel
    | VERSION AS OF TAG STRING_LITERAL              #tagVersionTravel
    ;

orderByList
    : sortItem (COMMA sortItem)*
    ;

sortItem
    : expression (ASC | DESC)? (NULLS (FIRST_KW | LAST_KW))?
    ;

// --- RERANK BY clause ---

rerankClause
    : rerankStrategy (ON LPAREN rerankColumnList RPAREN)?
    ;

rerankStrategy
    : RRF LPAREN expression RPAREN                                              #rrfStrategy
    | LINEAR_COMBINATION LPAREN expression COMMA expression RPAREN              #linearCombinationStrategy
    | CROSS_ENCODER LPAREN STRING_LITERAL COMMA expression RPAREN               #crossEncoderStrategy
    | CASCADE LPAREN rerankStrategy (COMMA rerankStrategy)* RPAREN              #cascadeStrategy
    ;

rerankColumnList
    : identifier (COMMA identifier)*
    ;

// --- GROUP BY clause with GROUPING SETS / CUBE / ROLLUP support ---

groupByClause
    : groupByElement (COMMA groupByElement)*
    ;

groupByElement
    : GROUPING_KW SETS LPAREN groupingSet (COMMA groupingSet)* RPAREN   #groupingSetsElement
    | CUBE LPAREN expressionList RPAREN                                  #cubeElement
    | ROLLUP LPAREN expressionList RPAREN                                #rollupElement
    | expression                                                         #groupByExpression
    ;

groupingSet
    : LPAREN expressionList? RPAREN
    | expression
    ;

// --- Expressions ---

expression
    : primaryExpression                                             #primaryExpr
    | expression LBRACKET expression RBRACKET                       #subscriptExpr
    | MINUS expression                                              #unaryMinusExpr
    | left=expression op=(STAR | SLASH | PERCENT) right=expression  #multiplicativeExpr
    | left=expression op=(PLUS | MINUS) right=expression            #additiveExpr
    | left=expression CONCAT_OP right=expression                    #concatExpr
    | left=expression op=comparisonOperator right=expression        #comparisonExpr
    | left=expression IN LPAREN query RPAREN                        #inSubqueryExpr
    | left=expression IN LPAREN expressionList RPAREN               #inListExpr
    | left=expression NOT IN LPAREN query RPAREN                    #notInSubqueryExpr
    | left=expression NOT IN LPAREN expressionList RPAREN           #notInListExpr
    | left=expression IS NOT? NULL                                  #isNullExpr
    | left=expression IS NOT? truth=(TRUE | FALSE | UNKNOWN)         #truthTestExpr
    | left=expression BETWEEN low=expression AND high=expression    #betweenExpr
    | left=expression LIKE right=expression                         #likeExpr
    | NOT expression                                                #notExpr
    | left=expression AND right=expression                          #andExpr
    | left=expression OR right=expression                           #orExpr
    | EXISTS LPAREN query RPAREN                                    #existsExpr
    | CASE expression (WHEN expression THEN expression)+ (ELSE expression)? END #simpleCaseExpr
    | CASE (WHEN expression THEN expression)+ (ELSE expression)? END #searchedCaseExpr
    ;

primaryExpression
    : literal                                                       #literalExpr
    | QUESTION                                                      #parameterMarkerExpr
    | CAST LPAREN expression AS castType RPAREN                     #castExpr
    | EXTRACT LPAREN extractField FROM expression RPAREN            #extractExpr
    | SUBSTRING LPAREN expression FROM expression FOR expression RPAREN #substringFromForExpr
    | ARRAY LBRACKET (expression (COMMA expression)*)? RBRACKET     #arrayConstructorExpr
    | MAP LBRACKET (expression (COMMA expression)*)? RBRACKET       #mapConstructorExpr
    | (STRUCT | ROW_KW) LPAREN namedField (COMMA namedField)* RPAREN #structConstructorExpr
    | qualifiedName LPAREN STAR RPAREN filterClause? windowSpec?    #starFunctionCallExpr
    | functionName LPAREN setQuantifier=DISTINCT? (expression (COMMA expression)*)? RPAREN filterClause? windowSpec? #functionCallExpr
    | qualifiedName                                                 #identifierExpr
    | LPAREN expression RPAREN                                      #parenExpr
    | LPAREN query RPAREN                                           #scalarSubqueryExpr
    ;

functionName
    : qualifiedName
    | AI_EMBED
    ;

namedField
    : identifier NAMED_ARROW expression                             #namedFieldWithName
    | expression                                                    #positionalField
    ;

extractField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

castType
    : BIGINT | INT | INTEGER | DOUBLE PRECISION_KW? | BOOLEAN | DATE | TIMESTAMP | STRING_TYPE
    | FLOAT | TINYINT | SMALLINT | VARBINARY | UUID
    | (DECIMAL | NUMERIC) (LPAREN precision=INTEGER_LITERAL (COMMA scale=INTEGER_LITERAL)? RPAREN)?
    | VARCHAR (LPAREN length=INTEGER_LITERAL RPAREN)?
    | ARRAY LPAREN castType RPAREN
    | MAP LPAREN castType COMMA castType RPAREN
    | STRUCT LPAREN structField (COMMA structField)* RPAREN
    ;

windowSpec
    : OVER LPAREN
        (PARTITION BY expressionList)?
        (ORDER BY orderByList)?
        windowFrame?
      RPAREN                                     #inlineWindowSpec
    | OVER windowName=identifier                 #namedWindowRef
    ;

// SQL:2003 aggregate filter: agg(...) FILTER (WHERE condition)
filterClause
    : FILTER LPAREN WHERE expression RPAREN
    ;

windowFrame
    : ROWS BETWEEN frameBound AND frameBound     #rowsBetweenFrame
    | ROWS frameBound                            #rowsFrame
    | RANGE BETWEEN frameBound AND frameBound    #rangeBetweenFrame
    | RANGE frameBound                           #rangeFrame
    ;

frameBound
    : UNBOUNDED PRECEDING                        #unboundedPreceding
    | UNBOUNDED FOLLOWING                        #unboundedFollowing
    | CURRENT ROW_KW                             #currentRow
    | expression PRECEDING                       #exprPreceding
    | expression FOLLOWING                       #exprFollowing
    ;

comparisonOperator
    : EQ | NEQ | LT | GT | LTE | GTE
    ;

literal
    : MINUS? INTEGER_LITERAL            #integerLiteral
    | MINUS? DECIMAL_LITERAL            #decimalLiteral
    | STRING_LITERAL                    #stringLiteral
    | TRUE                              #booleanTrue
    | FALSE                             #booleanFalse
    | NULL                              #nullLiteral
    | DATE STRING_LITERAL               #dateLiteral
    | UUID STRING_LITERAL               #uuidLiteral
    | TIMESTAMP WITH TIME ZONE STRING_LITERAL  #timestampWithTimeZoneLiteral
    | TIMESTAMP STRING_LITERAL          #timestampLiteral
    | INTERVAL STRING_LITERAL timeUnit (TO timeUnit)?  #intervalLiteral
    | HEX_LITERAL                       #binaryHexLiteral
    ;

timeUnit
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

expressionList
    : expression (COMMA expression)*
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

identifier
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    | BACKTICK_IDENTIFIER
    | DOLLAR_IDENTIFIER
    | nonReservedKeyword
    ;

// --- EXPLAIN ---

explainStatement
    : EXPLAIN statement
    ;

// --- Custom Extensions ---

compactTable
    : COMPACT qualifiedName
    ;

optimizeTable
    : OPTIMIZE qualifiedName
    ;

vacuumTable
    : VACUUM qualifiedName
    ;

// Collects connector statistics and persists them against the controlled table's
// current version token. COMPUTE STATISTICS is accepted as the explicit spelling.
analyzeTable
    : ANALYZE TABLE qualifiedName (COMPUTE STATISTICS)?
    ;

// --- Catalog DDL ---

createCatalog
    : CREATE CATALOG name=identifier USING connectorType=identifier WITH LPAREN propertyList RPAREN
    | CREATE CATALOG name=identifier (WITH | USING) PROPERTIES? LPAREN propertyList RPAREN
    ;

dropCatalog
    : DROP CATALOG identifier
    ;

showCatalogs
    : SHOW CATALOGS
    ;

showSchemas
    : SHOW SCHEMAS ((FROM | IN) identifier)?
    ;

showTables
    : SHOW TABLES ((FROM | IN) qualifiedName)?
    ;

// Ownership, identity, and version state of LINKED/MANAGED tables. With a
// catalog or schema scope the live catalog list is merged in as OBSERVED.
showTableControls
    : SHOW TABLE CONTROLS ((FROM | IN) qualifiedName)?
    ;

describeCatalog
    : DESCRIBE CATALOG identifier
    ;

describeTable
    : DESCRIBE TABLE? qualifiedName
    ;

alterCatalog
    : ALTER CATALOG identifier SET PROPERTIES LPAREN propertyList RPAREN
    ;

propertyList
    : property (COMMA property)*
    ;

property
    : key=identifier EQ value=literal
    | stringKey=STRING_LITERAL EQ value=literal
    ;

// --- ALTER TABLE ---

alterTable
    : ALTER TABLE qualifiedName ADD COLUMN? columnDefinition                          #alterTableAddColumn
    | ALTER TABLE qualifiedName DROP COLUMN? identifier                          #alterTableDropColumn
    | ALTER TABLE qualifiedName RENAME TO newName=qualifiedName                  #alterTableRename
    | ALTER TABLE qualifiedName RENAME COLUMN? old=identifier TO new_=identifier #alterTableRenameColumn
    | ALTER TABLE qualifiedName ALTER COLUMN? identifier (SET DATA)? TYPE columnType #alterTableChangeType
    | ALTER TABLE qualifiedName SET PARTITION SPEC LPAREN partitionTransformList RPAREN #alterTableSetPartitionSpec
    | ALTER TABLE qualifiedName SET SORT ORDER LPAREN sortColumnList RPAREN (USING ZORDER)?  #alterTableSetSortOrder
    | ALTER TABLE qualifiedName CREATE BRANCH branchName=identifier
      (RETAIN retentionDays=INTEGER_LITERAL DAYS)?                              #alterTableCreateBranch
    | ALTER TABLE qualifiedName DROP BRANCH (IF EXISTS)? branchName=identifier  #alterTableDropBranch
    | ALTER TABLE qualifiedName CREATE TAG tagName=identifier                    #alterTableCreateTag
    | ALTER TABLE qualifiedName DROP TAG (IF EXISTS)? tagName=identifier        #alterTableDropTag
    | ALTER TABLE qualifiedName ADOPT (WITH LPAREN propertyList RPAREN)?          #alterTableAdopt
    | ALTER TABLE qualifiedName UNLINK                                           #alterTableUnlink
    | ALTER TABLE qualifiedName ALTER COLUMN? columnName=identifier
            SET TAG tagKey=identifier EQ tagValue=STRING_LITERAL                 #alterTableSetColumnTag
    | ALTER TABLE qualifiedName ALTER COLUMN? columnName=identifier
            UNSET TAG tagKey=identifier                                          #alterTableUnsetColumnTag
    ;

sortColumnList
    : sortColumn (COMMA sortColumn)*
    ;

sortColumn
    : identifier (ASC | DESC)?
    ;

partitionTransformList
    : partitionTransform (COMMA partitionTransform)*
    ;

partitionTransform
    : identifier LPAREN expressionList RPAREN    #functionTransform
    | identifier                                  #identityTransform
    ;

// --- CALL statement ---

callStatement
    : CALL qualifiedName LPAREN (expression (COMMA expression)*)? RPAREN
    ;

// --- VIEW DDL ---

createView
    : CREATE (OR REPLACE)? VIEW (IF NOT EXISTS)? qualifiedName AS setQuery
    ;

dropView
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

showViews
    : SHOW VIEWS ((FROM | IN) qualifiedName)?
    ;

createSemanticView
    : CREATE SEMANTIC VIEW (IF NOT EXISTS)? qualifiedName
      VERSION version=INTEGER_LITERAL AS definition=STRING_LITERAL
    ;

publishSemanticView
    : ALTER SEMANTIC VIEW qualifiedName PUBLISH VERSION version=INTEGER_LITERAL
      (EXPECTING EPOCH expectedEpoch=INTEGER_LITERAL)?
    ;

showSemanticViews
    : SHOW SEMANTIC VIEWS ((FROM | IN) qualifiedName)?
    ;

createFeatureView
    : CREATE FEATURE VIEW (IF NOT EXISTS)? qualifiedName
      VERSION version=INTEGER_LITERAL AS definition=STRING_LITERAL
    ;

publishFeatureView
    : ALTER FEATURE VIEW qualifiedName PUBLISH VERSION version=INTEGER_LITERAL
      (EXPECTING EPOCH expectedEpoch=INTEGER_LITERAL)?
    ;

materializeFeatureView
    : MATERIALIZE FEATURE VIEW qualifiedName VERSION version=INTEGER_LITERAL
      SNAPSHOT snapshot=STRING_LITERAL
    ;

// --- SHOW metadata statements ---

showSnapshots
    : SHOW SNAPSHOTS (FROM | OF) qualifiedName
    ;

showFiles
    : SHOW FILES (FROM | OF) qualifiedName
    ;

showManifests
    : SHOW MANIFESTS (FROM | OF) qualifiedName
    ;

showPartitions
    : SHOW PARTITIONS (FROM | OF) qualifiedName
    ;

// --- Dynamic Table DDL ---

createDynamicTable
    : CREATE DYNAMIC TABLE qualifiedName
      (TARGET_LAG EQ STRING_LITERAL)?
      (REFRESH_MODE EQ dtRefreshMode)?
      (INCREMENTAL_STRATEGY EQ incrementalStrategy)?
      (WAREHOUSE EQ STRING_LITERAL)?
      AS query
      (WITH LPAREN stringPropertyList RPAREN)?
    ;

dtRefreshMode
    : SNAPSHOT       #snapshotRefreshMode
    | FULL           #fullRefreshMode
    | CDC            #cdcRefreshMode
    | AUTO           #autoRefreshMode
    | INCREMENTAL    #incrementalRefreshMode
    ;

incrementalStrategy
    : AUTO                 #autoIncrementalStrategy
    | APPEND_ONLY          #appendOnlyIncrementalStrategy
    | PARTITION_REPLACE    #partitionReplaceIncrementalStrategy
    | AFFECTED_KEY         #affectedKeyIncrementalStrategy
    | CDC                  #cdcIncrementalStrategy
    ;

alterDynamicTable
    : ALTER DYNAMIC TABLE qualifiedName SUSPEND                                 #alterDtSuspend
    | ALTER DYNAMIC TABLE qualifiedName RESUME                                  #alterDtResume
    | ALTER DYNAMIC TABLE qualifiedName REFRESH                                 #alterDtRefresh
    | ALTER DYNAMIC TABLE qualifiedName SET TARGET_LAG EQ STRING_LITERAL        #alterDtSetTargetLag
    | ALTER DYNAMIC TABLE qualifiedName SET REFRESH_MODE EQ dtRefreshMode       #alterDtSetRefreshMode
    | ALTER DYNAMIC TABLE qualifiedName RENAME TO qualifiedName                 #alterDtRename
    | ALTER DYNAMIC TABLE qualifiedName REDEFINE AS query                       #alterDtRedefine
    | ALTER DYNAMIC TABLE qualifiedName BACKFILL EMBEDDING columnName=identifier #alterDtBackfillEmbedding
    ;

dropDynamicTable
    : DROP DYNAMIC TABLE qualifiedName (CASCADE)?
    ;

showDynamicTables
    : SHOW DYNAMIC TABLES
    ;

// --- Materialized View DDL ---

createMaterializedView
    : CREATE (OR REPLACE)? MATERIALIZED VIEW (IF NOT EXISTS)? qualifiedName
      (TARGET_LAG EQ STRING_LITERAL)?
      (REFRESH_MODE EQ dtRefreshMode)?
      (INCREMENTAL_STRATEGY EQ incrementalStrategy)?
      AS query
    ;

alterMaterializedView
    : ALTER MATERIALIZED VIEW qualifiedName SUSPEND                              #alterMvSuspend
    | ALTER MATERIALIZED VIEW qualifiedName RESUME                               #alterMvResume
    | ALTER MATERIALIZED VIEW qualifiedName REFRESH                              #alterMvRefresh
    | ALTER MATERIALIZED VIEW qualifiedName SET TARGET_LAG EQ STRING_LITERAL     #alterMvSetTargetLag
    | ALTER MATERIALIZED VIEW qualifiedName SET REFRESH_MODE EQ dtRefreshMode    #alterMvSetRefreshMode
    | ALTER MATERIALIZED VIEW qualifiedName RENAME TO qualifiedName              #alterMvRename
    ;

refreshMaterializedView
    : REFRESH MATERIALIZED VIEW qualifiedName
    ;

dropMaterializedView
    : DROP MATERIALIZED VIEW (IF EXISTS)? qualifiedName (CASCADE)?
    ;

showMaterializedViews
    : SHOW MATERIALIZED VIEWS (LIKE STRING_LITERAL)?
    ;

describeMaterializedView
    : DESCRIBE MATERIALIZED VIEW qualifiedName
    ;

describeDynamicTable
    : DESCRIBE DYNAMIC TABLE qualifiedName
    ;

// --- Materialization observability ---

showMaterializations
    : SHOW MATERIALIZATIONS
    ;

showMaterializationRefreshHistory
    : SHOW MATERIALIZATION REFRESH HISTORY (FOR qualifiedName)?
    ;

showMaterializationDependencies
    : SHOW MATERIALIZATION DEPENDENCIES (FOR qualifiedName)?
    ;

showMaterializationGcHistory
    : SHOW MATERIALIZATION GC HISTORY (FOR qualifiedName)?
    ;

// --- Index DDL ---

createIndex
    : CREATE INDEX (IF NOT EXISTS)? indexName=identifier ON qualifiedName
      LPAREN columnNameList RPAREN USING indexTypeName=identifier
      (WITH LPAREN stringPropertyList RPAREN)? FORCE?
    ;

dropIndex
    : DROP INDEX (IF EXISTS)? indexName=identifier ON qualifiedName
    ;

showIndexes
    : SHOW INDEXES (FROM | ON) qualifiedName
    ;

alterIndex
    : ALTER INDEX indexName=identifier ON qualifiedName REBUILD
    ;

// --- Clustering DDL ---

setClusteringOrder
    : ALTER TABLE qualifiedName SET CLUSTERING ORDER
      LPAREN columnNameList RPAREN (MODE clusteringMode=(INTERLEAVE | LINEAR))?
    ;

reclusterTable
    : RECLUSTER TABLE qualifiedName (WHERE where=expression)?
    ;

showClusteringStatus
    : SHOW CLUSTERING STATUS FOR TABLE qualifiedName
    ;

// --- Stream DDL ---

createStream
    : CREATE STREAM (IF NOT EXISTS)? name=identifier
      FROM sourceProvider=identifier LPAREN streamPropertyList RPAREN
      FORMAT sourceFormat=STRING_LITERAL
      INTO targetTable=qualifiedName
      (WITH LPAREN streamPropertyList RPAREN)?
    ;

streamPropertyList
    : streamProperty (COMMA streamProperty)*
    ;

streamProperty
    : stringKey=STRING_LITERAL EQ stringValue=STRING_LITERAL        #streamStringProperty
    | identKey=identifier EQ identValue=(STRING_LITERAL | INTEGER_LITERAL)  #streamIdentProperty
    ;

alterStream
    : ALTER STREAM name=identifier SUSPEND                                  #alterStreamSuspend
    | ALTER STREAM name=identifier RESUME                                   #alterStreamResume
    | ALTER STREAM name=identifier SET LPAREN streamPropertyList RPAREN     #alterStreamSetProperties
    ;

dropStream
    : DROP STREAM (IF EXISTS)? name=identifier
    ;

showStreams
    : SHOW STREAMS
    ;

describeStream
    : DESCRIBE STREAM name=identifier
    ;

// --- Branch/Tag DDL ---

createBranch
    : CREATE BRANCH branchName=identifier ON tableName=qualifiedName
      (FROM TAG fromTag=identifier)?
      (FROM SNAPSHOT fromSnapshot=INTEGER_LITERAL)?
    ;

dropBranch
    : DROP BRANCH branchName=identifier ON tableName=qualifiedName
    ;

createTag
    : CREATE TAG tagName=identifier ON tableName=qualifiedName
    ;

dropTag
    : DROP TAG tagName=identifier ON tableName=qualifiedName
    ;

useBranch
    : USE BRANCH branchName=identifier ON tableName=qualifiedName
    | USE BRANCH MAIN ON tableName=qualifiedName
    ;

showBranches
    : SHOW BRANCHES (ON | FROM) tableName=qualifiedName
    ;

showTags
    : SHOW TAGS (ON | FROM) tableName=qualifiedName
    ;

// --- Embedding Model DDL ---

createEmbeddingModel
    : CREATE EMBEDDING MODEL modelName=identifier LPAREN embeddingPropertyList RPAREN
    ;

alterEmbeddingModel
    : ALTER EMBEDDING MODEL modelName=identifier SET LPAREN embeddingPropertyList RPAREN
    ;

dropEmbeddingModel
    : DROP EMBEDDING MODEL modelName=identifier (CASCADE)?
    ;

showEmbeddingModels
    : SHOW EMBEDDING MODELS
    ;

embeddingPropertyList
    : embeddingProperty (COMMA embeddingProperty)*
    ;

embeddingProperty
    : key=identifier EQ value=(STRING_LITERAL | INTEGER_LITERAL)
    ;

// --- Backfill Operations ---

backfillColumn
    : ALTER TABLE tableName=qualifiedName BACKFILL COLUMN columnName=identifier
      (WHERE where=expression)?
      (BATCH_SIZE batchSize=INTEGER_LITERAL)?
      (CONCURRENCY concurrency=INTEGER_LITERAL)?
    ;

cancelBackfill
    : CANCEL BACKFILL ON tableName=qualifiedName COLUMN columnName=identifier
    ;

showBackfillStatus
    : SHOW BACKFILL STATUS ON tableName=qualifiedName
    ;

// --- Storage Profile DDL ---

createStorageProfile
    : CREATE STORAGE PROFILE (IF NOT EXISTS)? profileName=identifier
      WITH LPAREN storageProfilePropertyList RPAREN
    ;

dropStorageProfile
    : DROP STORAGE PROFILE (IF EXISTS)? profileName=identifier
    ;

showStorageProfiles
    : SHOW STORAGE PROFILES
    ;

storageProfilePropertyList
    : storageProfileProperty (COMMA storageProfileProperty)*
    ;

storageProfileProperty
    : (key=identifier | skey=STRING_LITERAL) EQ value=expression
    ;

// --- Volume / Stage DDL ---

createVolume
    : CREATE (MANAGED | EXTERNAL)? VOLUME (IF NOT EXISTS)? qualifiedName volumeOption*
    ;

volumeOption
    : LOCATION STRING_LITERAL
    | STORAGE PROFILE EQ? STRING_LITERAL
    | STORAGE_PROFILE EQ? STRING_LITERAL
    | COMMENT STRING_LITERAL
    ;

alterVolume
    : ALTER VOLUME qualifiedName SET STORAGE PROFILE EQ? STRING_LITERAL
    | ALTER VOLUME qualifiedName SET STORAGE_PROFILE EQ? STRING_LITERAL
    | ALTER VOLUME qualifiedName SET COMMENT STRING_LITERAL
    | ALTER VOLUME qualifiedName SET OWNER TO identifier
    | ALTER VOLUME qualifiedName RENAME TO qualifiedName
    ;

dropVolume
    : DROP VOLUME (IF EXISTS)? qualifiedName
    ;

showVolumes
    : SHOW VOLUMES ((FROM | IN) qualifiedName)?
    ;

describeVolume
    : DESCRIBE VOLUME qualifiedName
    ;

createStage
    : CREATE (INTERNAL | EXTERNAL)? STAGE (IF NOT EXISTS)? qualifiedName stageOption*
    ;

stageOption
    : URL EQ STRING_LITERAL
    | STORAGE PROFILE EQ? STRING_LITERAL
    | STORAGE_PROFILE EQ? STRING_LITERAL
    | DIRECTORY EQ LPAREN ENABLE EQ (TRUE | FALSE) RPAREN
    | COMMENT EQ? STRING_LITERAL
    ;

alterStage
    : ALTER STAGE qualifiedName REFRESH (SUBPATH EQ STRING_LITERAL)?
    | ALTER STAGE qualifiedName SET URL EQ STRING_LITERAL
    | ALTER STAGE qualifiedName SET STORAGE PROFILE EQ? STRING_LITERAL
    | ALTER STAGE qualifiedName SET STORAGE_PROFILE EQ? STRING_LITERAL
    | ALTER STAGE qualifiedName SET DIRECTORY EQ LPAREN ENABLE EQ (TRUE | FALSE) RPAREN
    | ALTER STAGE qualifiedName SET COMMENT EQ? STRING_LITERAL
    | ALTER STAGE qualifiedName SET OWNER TO identifier
    | ALTER STAGE qualifiedName RENAME TO qualifiedName
    ;

dropStage
    : DROP STAGE (IF EXISTS)? qualifiedName
    ;

showStages
    : SHOW STAGES ((FROM | IN) qualifiedName)?
    ;

describeStage
    : DESCRIBE STAGE qualifiedName
    ;

listStage
    : LIST AT_SIGN stage=qualifiedName (SLASH path=stagePath)? SLASH?
      (PATTERN EQ pattern=STRING_LITERAL)?
    ;

removeStage
    : REMOVE AT_SIGN stage=qualifiedName (SLASH path=stagePath)? SLASH?
      (PATTERN EQ pattern=STRING_LITERAL)?
    ;

stagePath
    : stagePathSegment (SLASH stagePathSegment)*
    ;

stagePathSegment
    : (identifier | INTEGER_LITERAL) (DOT (identifier | INTEGER_LITERAL))*
    | STRING_LITERAL
    ;

// --- COPY INTO ---

copyInto
    : COPY INTO qualifiedName
      FROM source=copySource
      (FILE_FORMAT EQ (fileFormat=identifier | fileFormatStr=STRING_LITERAL))?
      (PATTERN EQ pattern=STRING_LITERAL)?
      (WITH LPAREN copyOptionList RPAREN)?
    ;

copySource
    : STRING_LITERAL         // URI: 's3://bucket/path/'
    | LPAREN setQuery RPAREN  // subquery
    ;

copyOptionList
    : copyOption (COMMA copyOption)*
    ;

copyOption
    : (key=identifier | skey=STRING_LITERAL) EQ value=expression
    ;

// --- SET session property ---

setSession
    : SET (propertyName=qualifiedName | propertyNameStr=STRING_LITERAL) EQ value=literal
    ;

// --- Table DDL ---

createTable
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
      LPAREN tableElementList RPAREN
      (PARTITIONED BY LPAREN partitionColumnList RPAREN)?
      (SORTED BY LPAREN sortColumnList RPAREN (USING ZORDER)?)?
      (COMMENT STRING_LITERAL)?
      (tableProperties)?                                            #createTableColumns
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
      AS setQuery                                                   #createTableAsSelect
    ;

createSchema
    : CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
    ;

tableElementList
    : tableElement (COMMA tableElement)*
    ;

tableElement
    : columnDefinition                                                  #columnDef
    | PRIMARY KEY LPAREN columnNameList RPAREN (NOT ENFORCED)?          #primaryKeyDef
    ;

columnDefinition
    : identifier columnType (NOT NULL)? (DEFAULT defaultVal=literal)? autoEmbeddingClause?
    ;

autoEmbeddingClause
    : GENERATED ALWAYS AS LPAREN AI_EMBED LPAREN modelName=STRING_LITERAL
      (COMMA sourceColumn+=identifier)+ RPAREN RPAREN
      (EMBEDDING MODE embeddingMode=(SYNC | ASYNC))?
    ;

columnType
    : BIGINT | INT | INTEGER | DOUBLE PRECISION_KW? | BOOLEAN | DATE | TIMESTAMP | STRING_TYPE | UUID
    | VARCHAR (LPAREN length=INTEGER_LITERAL RPAREN)?
    | VARBINARY
    | DECIMAL (LPAREN precision=INTEGER_LITERAL (COMMA scale=INTEGER_LITERAL)? RPAREN)?
    | NUMERIC (LPAREN precision=INTEGER_LITERAL (COMMA scale=INTEGER_LITERAL)? RPAREN)?
    | FLOAT
    | TINYINT
    | SMALLINT
    | VARIANT
    | GEOMETRY
    | GEOGRAPHY
    | TIMESTAMP_NS
    | BLOB
    | FILE_TYPE_KW (MANAGED | EXTERNAL)?
    | VECTOR LPAREN vectorDim=INTEGER_LITERAL (COMMA vectorElementType=(FLOAT | DOUBLE))? RPAREN
    | ARRAY LPAREN elementType=columnType RPAREN
    | MAP LPAREN keyType=columnType COMMA valueType=columnType RPAREN
    | STRUCT LPAREN structField (COMMA structField)* RPAREN
    ;

structField
    : fieldName=identifier fieldType=columnType
    ;

partitionColumnList
    : identifier (COMMA identifier)*
    ;

tableProperties
    : TBLPROPERTIES LPAREN stringPropertyList RPAREN
    | WITH LPAREN stringPropertyList RPAREN
    ;

stringPropertyList
    : stringProperty (COMMA stringProperty)*
    ;

stringProperty
    : key=STRING_LITERAL EQ value=STRING_LITERAL
    ;

dropTable
    : DROP TABLE (IF EXISTS)? qualifiedName
    ;

// --- INSERT INTO (VALUES) ---

insertInto
    : INSERT INTO qualifiedName (LPAREN columnNameList RPAREN)?
      VALUES valuesList
    ;

// --- INSERT OVERWRITE ---

insertOverwrite
    : INSERT OVERWRITE qualifiedName (LPAREN columnNameList RPAREN)?
      (PARTITION LPAREN partitionSpec RPAREN)?
      VALUES valuesList
    ;

partitionSpec
    : partitionAssignment (COMMA partitionAssignment)*
    ;

partitionAssignment
    : identifier EQ literal
    ;

// --- INSERT INTO ... SELECT ---

insertSelect
    : INSERT (INTO | OVERWRITE) qualifiedName (LPAREN columnNameList RPAREN)?
      setQuery
    ;

columnNameList
    : identifier (COMMA identifier)*
    ;

valuesList
    : valueTuple (COMMA valueTuple)*
    ;

valueTuple
    : LPAREN expression (COMMA expression)* RPAREN
    ;

// --- DELETE FROM ---

deleteFrom
    : DELETE FROM qualifiedName (WHERE expression)?
    ;

// --- UPDATE ---

updateStatement
    : UPDATE qualifiedName SET setClauseList (WHERE expression)?
    ;

setClauseList
    : setClause (COMMA setClause)*
    ;

setClause
    : identifier EQ expression
    ;

// --- MERGE INTO ---

mergeInto
    : MERGE INTO qualifiedName (AS? alias=identifier)?
      USING tableRef ON expression
      mergeClause+
    ;

mergeClause
    : WHEN MATCHED (AND expression)? THEN mergeUpdate                   #mergeWhenMatched
    | WHEN NOT MATCHED (AND expression)? THEN mergeInsert               #mergeWhenNotMatched
    | WHEN MATCHED (AND expression)? THEN DELETE                        #mergeWhenMatchedDelete
    ;

mergeUpdate
    : UPDATE SET setClauseList
    ;

mergeInsert
    : INSERT (LPAREN columnNameList RPAREN)? VALUES valueTuple
    ;

// ============================================================
// Lexer Rules
// ============================================================

// Keywords
SELECT      : S E L E C T;
FROM        : F R O M;
WHERE       : W H E R E;
GROUP       : G R O U P;
BY          : B Y;
HAVING      : H A V I N G;
ORDER       : O R D E R;
LIMIT       : L I M I T;
OFFSET      : O F F S E T;
AS          : A S;
ON          : O N;
AND         : A N D;
OR          : O R;
NOT         : N O T;
TRUE        : T R U E;
FALSE       : F A L S E;
UNKNOWN     : U N K N O W N;
NULL        : N U L L;
ASC         : A S C;
DESC        : D E S C;

// Join keywords
JOIN        : J O I N;
INNER       : I N N E R;
LEFT        : L E F T;
RIGHT       : R I G H T;
FULL        : F U L L;
CROSS       : C R O S S;
OUTER       : O U T E R;

// Set operation keywords
UNION       : U N I O N;
ALL         : A L L;
INTERSECT   : I N T E R S E C T;
EXCEPT      : E X C E P T;

// Subquery / window keywords
IN          : I N;
DISTINCT    : D I S T I N C T;
OVER        : O V E R;
PARTITION   : P A R T I T I O N;
FILTER      : F I L T E R;
INTEGER     : I N T E G E R;

// Custom extension keywords
REFRESH     : R E F R E S H;
MATERIALIZED: M A T E R I A L I Z E D;
MATERIALIZATION : M A T E R I A L I Z A T I O N;
MATERIALIZATIONS : M A T E R I A L I Z A T I O N S;
INCREMENTAL_STRATEGY : I N C R E M E N T A L '_' S T R A T E G Y;
APPEND_ONLY : A P P E N D '_' O N L Y;
PARTITION_REPLACE : P A R T I T I O N '_' R E P L A C E;
AFFECTED_KEY : A F F E C T E D '_' K E Y;
HISTORY     : H I S T O R Y;
DEPENDENCIES : D E P E N D E N C I E S;
GC          : G C;
VIEW        : V I E W;
SEMANTIC    : S E M A N T I C;
FEATURE     : F E A T U R E;
MATERIALIZE : M A T E R I A L I Z E;
PUBLISH     : P U B L I S H;
EXPECTING   : E X P E C T I N G;
EPOCH       : E P O C H;
INCREMENTAL : I N C R E M E N T A L;
COMPACT     : C O M P A C T;
OPTIMIZE    : O P T I M I Z E;
VACUUM      : V A C U U M;
ANALYZE     : A N A L Y Z E;
COMPUTE     : C O M P U T E;
STATISTICS  : S T A T I S T I C S;

// Catalog DDL keywords
CREATE      : C R E A T E;
DROP        : D R O P;
SHOW        : S H O W;
CATALOGS    : C A T A L O G S;
CATALOG     : C A T A L O G;
DESCRIBE    : D E S C R I B E;
ALTER       : A L T E R;
SET         : S E T;
PROPERTIES  : P R O P E R T I E S;
WITH        : W I T H;

// CALL keyword
CALL        : C A L L;
CAST        : C A S T;
EXTRACT     : E X T R A C T;
INTERVAL    : I N T E R V A L;
SUBSTRING   : S U B S T R I N G;
EXPLAIN     : E X P L A I N;
SPEC        : S P E C;

// GROUPING SETS / CUBE / ROLLUP keywords
GROUPING_KW : G R O U P I N G;
SETS        : S E T S;
CUBE        : C U B E;
ROLLUP      : R O L L U P;

// LATERAL / EXPLODE keywords
LATERAL         : L A T E R A L;
EXPLODE         : E X P L O D E;
POSEXPLODE      : P O S E X P L O D E;
GENERATE_SERIES : G E N E R A T E '_' S E R I E S;
UNNEST          : U N N E S T;
ORDINALITY      : O R D I N A L I T Y;

// Table DDL keywords
TABLE       : T A B L E;
IF          : I F;
EXISTS      : E X I S T S;
INTO        : I N T O;
VALUES      : V A L U E S;
INSERT      : I N S E R T;
PARTITIONED : P A R T I T I O N E D;
COMMENT     : C O M M E N T;
TBLPROPERTIES : T B L P R O P E R T I E S;

// Column type keywords
BIGINT      : B I G I N T;
INT         : I N T;
VARCHAR     : V A R C H A R;
VARBINARY   : V A R B I N A R Y;
DOUBLE      : D O U B L E;
BOOLEAN     : B O O L E A N;
DATE        : D A T E;
UUID        : U U I D;
TIMESTAMP   : T I M E S T A M P;
STRING_TYPE : S T R I N G;
DECIMAL     : D E C I M A L;
NUMERIC     : N U M E R I C;
FLOAT       : F L O A T;
PRECISION_KW: P R E C I S I O N;
TINYINT     : T I N Y I N T;
SMALLINT    : S M A L L I N T;
VECTOR      : V E C T O R;
ARRAY       : A R R A Y;
MAP         : M A P;
STRUCT      : S T R U C T;
VARIANT     : V A R I A N T;
GEOMETRY    : G E O M E T R Y;
GEOGRAPHY   : G E O G R A P H Y;
TIMESTAMP_NS: T I M E S T A M P '_' N S;
BLOB        : B L O B;
FILE_TYPE_KW: F I L E;

// Constraint keywords
PRIMARY     : P R I M A R Y;
KEY         : K E Y;
ENFORCED    : E N F O R C E D;

// Misc keywords
OF          : O F;

// Time travel keywords
FOR         : F O R;
SYSTEM_TIME : S Y S T E M '_' T I M E;
SYSTEM_VERSION : S Y S T E M '_' V E R S I O N;
SYSTEM_KW   : S Y S T E M;
VERSION     : V E R S I O N;

// Dynamic Table keywords
DYNAMIC     : D Y N A M I C;
TARGET_LAG  : T A R G E T '_' L A G;
REFRESH_MODE: R E F R E S H '_' M O D E;
WAREHOUSE   : W A R E H O U S E;
REDEFINE    : R E D E F I N E;
SUSPEND     : S U S P E N D;
RESUME      : R E S U M E;
PRIVILEGE   : P R I V I L E G E;
MUTATION    : M U T A T I O N;
GRANT       : G R A N T;
REVOKE      : R E V O K E;
ROLE        : R O L E;
TENANT      : T E N A N T;
OPTION      : O P T I O N;
PRIVILEGES  : P R I V I L E G E S;
RESTRICT    : R E S T R I C T;
CASCADE     : C A S C A D E;
CDC         : C D C;
AUTO        : A U T O;
SNAPSHOT    : S N A P S H O T;

// DML keywords
DELETE      : D E L E T E;
UPDATE      : U P D A T E;
MERGE       : M E R G E;
USING       : U S I N G;
WHEN        : W H E N;
MATCHED     : M A T C H E D;
THEN        : T H E N;
IS          : I S;

// Index DDL keywords
INDEX       : I N D E X;
INDEXES     : I N D E X E S;
REBUILD     : R E B U I L D;
FORCE       : F O R C E;

// Clustering DDL keywords
CLUSTERING  : C L U S T E R I N G;
RECLUSTER   : R E C L U S T E R;
INTERLEAVE  : I N T E R L E A V E;
LINEAR      : L I N E A R;
MODE        : M O D E;
STATUS      : S T A T U S;

// Full-text search keywords
MATCH       : M A T C H;

// Embedding Model keywords
EMBEDDING   : E M B E D D I N G;
MODELS      : M O D E L S;
MODEL       : M O D E L;
GENERATED   : G E N E R A T E D;
ALWAYS      : A L W A Y S;
AI_EMBED    : A I '_' E M B E D;
SYNC        : S Y N C;
ASYNC       : A S Y N C;

// Rerank keywords
RERANK      : R E R A N K;
RRF         : R R F;
LINEAR_COMBINATION : L I N E A R '_' C O M B I N A T I O N;
CROSS_ENCODER : C R O S S '_' E N C O D E R;

// Backfill keywords
BACKFILL    : B A C K F I L L;
BATCH_SIZE  : B A T C H '_' S I Z E;
CONCURRENCY : C O N C U R R E N C Y;
CANCEL      : C A N C E L;

// Storage Profile and COPY INTO keywords
STORAGE     : S T O R A G E;
STORAGE_PROFILE : S T O R A G E '_' P R O F I L E;
PROFILE     : P R O F I L E;
PROFILES    : P R O F I L E S;
COPY        : C O P Y;
FILE_FORMAT : F I L E '_' F O R M A T;
PATTERN     : P A T T E R N;

// Volume and Stage keywords
LIST        : L I S T;
REMOVE      : R E M O V E;
VOLUME      : V O L U M E;
VOLUMES     : V O L U M E S;
STAGE       : S T A G E;
STAGES      : S T A G E S;
MANAGED     : M A N A G E D;
EXTERNAL    : E X T E R N A L;
INTERNAL    : I N T E R N A L;
LOCATION    : L O C A T I O N;
URL         : U R L;
OWNER       : O W N E R;
DIRECTORY   : D I R E C T O R Y;
ENABLE      : E N A B L E;
SUBPATH     : S U B P A T H;

// Branch retention keywords
RETAIN      : R E T A I N;
DAYS        : D A Y S;

// View replacement keyword
REPLACE     : R E P L A C E;

// Temporal keywords for TIMESTAMP WITH TIME ZONE
TIME        : T I M E;
ZONE        : Z O N E;

// CTE keywords
RECURSIVE   : R E C U R S I V E;
SEARCH      : S E A R C H;
DEPTH       : D E P T H;
BREADTH     : B R E A D T H;
CYCLE       : C Y C L E;

// QUALIFY keyword
QUALIFY     : Q U A L I F Y;

// TABLESAMPLE keywords
TABLESAMPLE : T A B L E S A M P L E;
BERNOULLI   : B E R N O U L L I;
REPEATABLE  : R E P E A T A B L E;

// PIVOT/UNPIVOT keywords
PIVOT       : P I V O T;
UNPIVOT     : U N P I V O T;
INCLUDE     : I N C L U D E;
EXCLUDE     : E X C L U D E;
NULLS       : N U L L S;
FIRST_KW    : F I R S T;
LAST_KW     : L A S T;

// BLOB function keywords
FROM_FILE       : F R O M '_' F I L E;
TO_FILE         : T O '_' F I L E;
BLOB_SIZE       : B L O B '_' S I Z E;
BLOB_MIME_TYPE  : B L O B '_' M I M E '_' T Y P E;

// Feature engineering function keywords
TOKENIZE        : T O K E N I Z E;
NORMALIZE       : N O R M A L I Z E;
ONE_HOT_ENCODE  : O N E '_' H O T '_' E N C O D E;
BUCKETIZE       : B U C K E T I Z E;

// Stream DDL keywords
STREAM      : S T R E A M;
STREAMS     : S T R E A M S;
KAFKA       : K A F K A;
FORMAT      : F O R M A T;
CHANGES     : C H A N G E S;

// Additional keywords
OVERWRITE   : O V E R W R I T E;
ADD         : A D D;
COLUMN      : C O L U M N;
RENAME      : R E N A M E;
TO          : T O;
SCHEMAS     : S C H E M A S;
SCHEMA      : S C H E M A;
TABLES      : T A B L E S;
CONTROLS    : C O N T R O L S;
DATA        : D A T A;
DEFAULT     : D E F A U L T;
TYPE        : T Y P E;
LIKE        : L I K E;
BETWEEN     : B E T W E E N;
CASE        : C A S E;
ELSE        : E L S E;
END         : E N D;

// View and metadata keywords
VIEWS       : V I E W S;
SNAPSHOTS   : S N A P S H O T S;
FILES       : F I L E S;
MANIFESTS   : M A N I F E S T S;
PARTITIONS  : P A R T I T I O N S;
BRANCH      : B R A N C H;
BRANCHES    : B R A N C H E S;
TAG         : T A G;
TAGS        : T A G S;
ADOPT       : A D O P T;
UNLINK      : U N L I N K;
MAIN        : M A I N;
USE         : U S E;
UNSET       : U N S E T;
SORTED      : S O R T E D;
SORT        : S O R T;
ZORDER      : Z O R D E R;

// Window frame keywords
ROWS        : R O W S;
RANGE       : R A N G E;
UNBOUNDED   : U N B O U N D E D;
PRECEDING   : P R E C E D I N G;
FOLLOWING   : F O L L O W I N G;
CURRENT     : C U R R E N T;
ROW_KW      : R O W;

// WINDOW clause keyword
WINDOW      : W I N D O W;

// Time unit keywords
YEAR        : Y E A R;
MONTH       : M O N T H;
DAY         : D A Y;
HOUR        : H O U R;
MINUTE      : M I N U T E;
SECOND      : S E C O N D;

// Operators
AT_SIGN     : '@';
CONCAT_OP   : '||';
NAMED_ARROW : '=>';
STAR        : '*';
SLASH       : '/';
PERCENT     : '%';
PLUS        : '+';
MINUS       : '-';
EQ          : '=';
NEQ         : '<>';
LT          : '<';
GT          : '>';
LTE         : '<=';
GTE         : '>=';
QUESTION    : '?';

// Delimiters
LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '[';
RBRACKET    : ']';
COMMA       : ',';
SEMICOLON   : ';';
DOT         : '.';
DOLLAR      : '$';

// Literals
INTEGER_LITERAL
    : DIGIT+
    ;

DECIMAL_LITERAL
    : DIGIT+ DOT DIGIT* EXPONENT?
    | DOT DIGIT+ EXPONENT?
    | DIGIT+ EXPONENT
    ;

fragment EXPONENT
    : [eE] [+-]? DIGIT+
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

HEX_LITERAL
    : X '\'' ([0-9a-fA-F])* '\''
    ;

// Identifiers — DOLLAR_IDENTIFIER must precede IDENTIFIER for correct matching
DOLLAR_IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')* DOLLAR (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKTICK_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

// Whitespace and comments
WS
    : [ \t\r\n]+ -> skip
    ;

LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;

// Case-insensitive character fragments
fragment A : [aA]; fragment B : [bB]; fragment C : [cC]; fragment D : [dD];
fragment E : [eE]; fragment F : [fF]; fragment G : [gG]; fragment H : [hH];
fragment I : [iI]; fragment J : [jJ]; fragment K : [kK]; fragment L : [lL];
fragment M : [mM]; fragment N : [nN]; fragment O : [oO]; fragment P : [pP];
fragment Q : [qQ]; fragment R : [rR]; fragment S : [sS]; fragment T : [tT];
fragment U : [uU]; fragment V : [vV]; fragment W : [wW]; fragment X : [xX];
fragment Y : [yY]; fragment Z : [zZ];

fragment DIGIT  : [0-9];
fragment LETTER : [a-zA-Z];

// Non-reserved keywords (can be used as identifiers)
nonReservedKeyword
    : REFRESH | MATERIALIZED | MATERIALIZE | VIEW | SEMANTIC | FEATURE
    | MATERIALIZATION | MATERIALIZATIONS | HISTORY | DEPENDENCIES | GC
    | INCREMENTAL_STRATEGY | APPEND_ONLY | PARTITION_REPLACE | AFFECTED_KEY
    | UNKNOWN | ROLE | TENANT | OPTION | PRIVILEGES | RESTRICT | PRIVILEGE | MUTATION
    | PUBLISH | EXPECTING | EPOCH
    | INCREMENTAL | COMPACT | OPTIMIZE | VACUUM | ANALYZE | COMPUTE | STATISTICS
    | CATALOG | CATALOGS | DESCRIBE | PROPERTIES | CALL
    | SYSTEM_TIME | SYSTEM_VERSION | VERSION | SET | WITH | SHOW | LIMIT
    | TABLE | IF | EXISTS | INTO | VALUES | INSERT | PARTITIONED | COMMENT
    | TBLPROPERTIES | BIGINT | INT | VARCHAR | VARBINARY | DOUBLE | BOOLEAN | DATE | OFFSET
    | TIMESTAMP | STRING_TYPE | INTEGER | UUID
    | OUTER | OVER | PARTITION | FILTER
    | DECIMAL | NUMERIC | FLOAT | TINYINT | SMALLINT | VECTOR | ARRAY | MAP | STRUCT
    | PRIMARY | KEY | ENFORCED
    | DELETE | UPDATE | MERGE | USING | MATCHED | THEN | IS
    | OVERWRITE | ADD | COLUMN | RENAME | TO | SCHEMAS | SCHEMA | TABLES | CONTROLS
    | DATA | TYPE | LIKE | BETWEEN | CASE | ELSE | END | DEFAULT
    | ROWS | UNBOUNDED | PRECEDING | FOLLOWING | CURRENT | ROW_KW
    | INTERSECT | EXCEPT
    | DYNAMIC | TARGET_LAG | REFRESH_MODE | WAREHOUSE | REDEFINE
    | SUSPEND | RESUME | CASCADE | CDC | AUTO | SNAPSHOT
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | INTERVAL | EXTRACT | SUBSTRING
    | EXPLAIN | SPEC
    | GROUPING_KW | SETS | CUBE | ROLLUP
    | LATERAL | EXPLODE | POSEXPLODE | GENERATE_SERIES
    | INDEX | INDEXES | REBUILD | FORCE
    | CLUSTERING | RECLUSTER | INTERLEAVE | LINEAR | MODE | STATUS | FOR
    | STREAM | STREAMS | KAFKA | FORMAT
    | VARIANT | GEOMETRY | GEOGRAPHY | TIMESTAMP_NS
    | VIEWS | SNAPSHOTS | FILES | MANIFESTS | PARTITIONS
    | BRANCH | TAG | SORTED | SORT | ZORDER
    | BRANCHES | TAGS | MAIN | USE | UNSET
    | CHANGES | MATCH
    | EMBEDDING | MODEL | MODELS
    | GENERATED | ALWAYS | SYNC | ASYNC
    | RERANK | RRF | LINEAR_COMBINATION | CROSS_ENCODER
    | BACKFILL | BATCH_SIZE | CONCURRENCY | CANCEL
    | BLOB | FILE_TYPE_KW | FROM_FILE | TO_FILE | BLOB_SIZE | BLOB_MIME_TYPE
    | TOKENIZE | NORMALIZE | ONE_HOT_ENCODE | BUCKETIZE
    | RECURSIVE
    | SEARCH | DEPTH | BREADTH | CYCLE
    | TIME | ZONE
    | QUALIFY
    | TABLESAMPLE | BERNOULLI | REPEATABLE | SYSTEM_KW
    | WINDOW | RANGE
    | PIVOT | UNPIVOT | INCLUDE | EXCLUDE | NULLS | FIRST_KW | LAST_KW
    | STORAGE | STORAGE_PROFILE | PROFILE | PROFILES | COPY | FILE_FORMAT | PATTERN
    | VOLUME | VOLUMES | STAGE | STAGES | MANAGED | EXTERNAL | INTERNAL
    | LOCATION | URL | OWNER | DIRECTORY | ENABLE | SUBPATH | LIST | REMOVE
    | RETAIN | DAYS | REPLACE
    ;
