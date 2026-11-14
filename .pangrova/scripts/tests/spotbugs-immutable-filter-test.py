#!/usr/bin/env python3
"""Verify narrow evidence-backed classifications with the pinned SpotBugs Filter API."""

import os
from copy import deepcopy
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[3]
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}

SOURCE = '''
import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.filter.Filter;
import edu.umd.cs.findbugs.filter.SignatureUtil;
import edu.umd.cs.findbugs.Plugin;

public final class ImmutableFilterBoundary {
    private static final String OWNER =
            "org.pangrova.protocol.function.declaration.FunctionBindingDeclarations";
    private static final String BYTES = "Lcom/google/protobuf/ByteString;";

    private static void check(Filter filter, String owner, String method, String parameters,
            String result, String field, String type, String bug, boolean expected) {
        BugInstance finding = new BugInstance(bug, 2).addClass(owner)
                .addMethod(owner, method, "(" + parameters + ")" + result, true)
                .addField(owner, field, type, false);
        if (filter.match(finding) != expected) {
            throw new AssertionError("Unexpected classification: " + owner + "." + method
                    + " result=" + result + " field=" + field + " type=" + type + " bug=" + bug);
        }
    }

    public static void main(String[] args) throws Exception {
        Plugin.loadCustomPlugin(new java.io.File(args[2]), null);
        Filter filter = Filter.parseFilter(args[0]);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", true);
        check(filter, OWNER, "declarationSha256", "", "[B", "declarationSha256", "[B", "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", "[B", "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", "[B", "MS_EXPOSE_REP", false);
        check(filter, OWNER, "otherHash", "", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "otherHash", BYTES, "MS_EXPOSE_REP", false);
        check(filter, OWNER + "Other", "declarationSha256", "", BYTES, "declarationSha256", BYTES,
                "MS_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "", BYTES, "declarationSha256", BYTES, "EI_EXPOSE_REP", false);
        check(filter, OWNER, "declarationSha256", "I", BYTES, "declarationSha256", BYTES, "MS_EXPOSE_REP", false);
        Filter command = Filter.parseFilter(args[1]);
        String runner = "org.pangrova.testing.testkit.security.TestTlsAuthority";
        command(command, runner, "run", "(Ljava/util/List;)V", "COMMAND_INJECTION", true);
        command(command, runner + "Other", "run", "(Ljava/util/List;)V", "COMMAND_INJECTION", false);
        command(command, runner, "execute", "(Ljava/util/List;)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/lang/String;)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "()V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;I)V", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;)Ljava/lang/Process;", "COMMAND_INJECTION", false);
        command(command, runner, "run", "(Ljava/util/List;)V", "OVERLY_PERMISSIVE_FILE_PERMISSION", false);
        Filter jitter = Filter.parseFilter(args[3]);
        String policy = "org.pangrova.common.client.session.RetryPolicy";
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/time/Duration;", "PREDICTABLE_RANDOM", true);
        command(jitter, policy + "Other", "delayForAttemptWithJitter", "(I)Ljava/time/Duration;",
                "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "token", "(I)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(J)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "()Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(II)Ljava/time/Duration;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/lang/String;", "PREDICTABLE_RANDOM", false);
        command(jitter, policy, "delayForAttemptWithJitter", "(I)Ljava/time/Duration;", "HARD_CODE_KEY", false);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(args[4]))) {
            String[] part = line.split("\\\\|", -1);
            Filter borrowed = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2];
            String signature = SignatureUtil.createMethodSignature(part[3], part[4]);
            int parameterEnd = signature.indexOf(')');
            String params = signature.substring(1, parameterEnd), result = signature.substring(parameterEnd + 1);
            String field = part[5], type = SignatureUtil.createFieldSignature(part[6]), bug = part[7];
            check(borrowed, owner, method, params, result, field, type, bug, true);
            check(borrowed, owner + "Other", method, params, result, field, type, bug, false);
            check(borrowed, owner, method + "Other", params, result, field, type, bug, false);
            check(borrowed, owner, method, params + "I", result, field, type, bug, false);
            check(borrowed, owner, method, "Ljava/lang/Object;", result, field, type, bug, false);
            check(borrowed, owner, method, params, "[B", field, type, bug, false);
            check(borrowed, owner, method, params, result, field + "Other", type, bug, false);
            check(borrowed, owner, method, params, result, field, "[B", bug, false);
            check(borrowed, owner, method, params, result, field, type,
                    bug.equals("EI_EXPOSE_REP") ? "EI_EXPOSE_REP2" : "EI_EXPOSE_REP", false);
            check(borrowed, owner, method, params, result, field, type, "HARD_CODE_KEY", false);
        }
        verifyJdbcForwards(args[5], args[6]);
        verifyImmutableAccessors(args[7], args[6]);
        verifyImmutableAccessors(args[8], args[6]);
        verifyImmutableAccessors(args[9], args[6]);
        verifyIndexingBorrowed(args[6]);
        verifyManagedSourceBorrowed(args[6]);
        verifyImmutableResourceGrants(args[6]);
        verifyParserBorrowing(args[6]);
        verifyQueryOwnership(args[6]);
        verifyPolicyStoreConstructor(args[6]);
        verifyTableStoreBorrowing(args[6]);
    }

    private static void verifyTableStoreBorrowing(String path) throws Exception {
        Filter full = Filter.parseFilter(path);
        String[] stores = {"control.JdbcTableControlStore", "maintenance.JdbcTableControlMaintenanceStore",
                "statistics.JdbcTableControlStatisticsStore", "policy.PostgresTablePolicyRepository"};
        String type = "Ljavax/sql/DataSource;";
        for (String store : stores) {
            String owner = "org.pangrova.persistence.table.postgresql." + store;
            check(full, owner, "<init>", type, "V", "dataSource", type, "EI_EXPOSE_REP2", true);
            check(full, owner + "Other", "<init>", type, "V", "dataSource", type, "EI_EXPOSE_REP2", false);
            check(full, owner, "create", type, "V", "dataSource", type, "EI_EXPOSE_REP2", false);
            check(full, owner, "<init>", type + "I", "V", "dataSource", type, "EI_EXPOSE_REP2", false);
            check(full, owner, "<init>", type, "V", "other", type, "EI_EXPOSE_REP2", false);
            check(full, owner, "<init>", type, "V", "dataSource", "[B", "EI_EXPOSE_REP2", false);
            check(full, owner, "<init>", type, "V", "dataSource", type, "EI_EXPOSE_REP", false);
            check(full, owner, "<init>", type, "V", "dataSource", type, "SQL_INJECTION_JDBC", false);
            check(full, "org.pangrova.persistence.jdbc.table." + store, "<init>", type, "V",
                    "dataSource", type, "EI_EXPOSE_REP2", false);
        }
    }

    private static void verifyPolicyStoreConstructor(String path) throws Exception {
        Filter full = Filter.parseFilter(path);
        String owner = "org.pangrova.persistence.security.postgresql.support.JdbcPolicyStoreSupport";
        String signature = "(Ljavax/sql/DataSource;)V";
        command(full, owner, "<init>", signature, "CT_CONSTRUCTOR_THROW", true);
        command(full, owner + "Other", "<init>", signature, "CT_CONSTRUCTOR_THROW", false);
        command(full, owner, "create", signature, "CT_CONSTRUCTOR_THROW", false);
        command(full, owner, "<init>", "(Ljava/sql/Connection;)V", "CT_CONSTRUCTOR_THROW", false);
        command(full, owner, "<init>", "()V", "CT_CONSTRUCTOR_THROW", false);
        command(full, owner, "<init>", signature, "SQL_INJECTION_JDBC", false);
        command(full, owner, "execute", "(Ljava/lang/String;)V", "SQL_INJECTION_JDBC", false);
        command(full, "org.pangrova.persistence.jdbc.security.support.JdbcPolicyStoreSupport",
                "<init>", signature, "CT_CONSTRUCTOR_THROW", false);
    }

    private static void verifyQueryOwnership(String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        String[][] cases = {
            {"org.pangrova.query.lifecycle.QueryTerminalCleanup$Context", "state", "", "org.pangrova.query.lifecycle.QueryStateMachine", "state", "org.pangrova.query.lifecycle.QueryStateMachine", "EI_EXPOSE_REP"},
            {"org.pangrova.query.lifecycle.QueryTerminalCleanup$Context", "<init>", "java.lang.String,org.pangrova.query.lifecycle.QueryStateMachine,java.lang.String", "void", "state", "org.pangrova.query.lifecycle.QueryStateMachine", "EI_EXPOSE_REP2"},
            {"org.pangrova.query.retention.QueryRetentionScheduler", "<init>", "java.util.concurrent.ConcurrentHashMap,java.util.function.Predicate,java.lang.Runnable", "void", "queries", "java.util.concurrent.ConcurrentHashMap", "EI_EXPOSE_REP2"},
            {"org.pangrova.execution.stage.binding.RerankIntent", "modelLimits", "", "org.pangrova.protocol.v2.CapabilityLimits", "modelLimits", "org.pangrova.protocol.v2.CapabilityLimits", "EI_EXPOSE_REP"},
            {"org.pangrova.execution.stage.binding.RerankIntent", "<init>", "int,int,java.lang.String,java.lang.String,byte[],org.pangrova.protocol.v2.LogicalType,org.pangrova.protocol.v2.CapabilityLimits,java.lang.String,java.lang.String", "void", "modelLimits", "org.pangrova.protocol.v2.CapabilityLimits", "EI_EXPOSE_REP2"},
            {"org.pangrova.execution.stage.binding.RerankIntent", "modelOutputType", "", "org.pangrova.protocol.v2.LogicalType", "modelOutputType", "org.pangrova.protocol.v2.LogicalType", "EI_EXPOSE_REP"},
            {"org.pangrova.execution.stage.binding.RerankIntent", "<init>", "int,int,java.lang.String,java.lang.String,byte[],org.pangrova.protocol.v2.LogicalType,org.pangrova.protocol.v2.CapabilityLimits,java.lang.String,java.lang.String", "void", "modelOutputType", "org.pangrova.protocol.v2.LogicalType", "EI_EXPOSE_REP2"},
            {"org.pangrova.execution.stage.binding.RetrievalGenerationIntent", "capability", "", "org.pangrova.protocol.v2.CapabilityRef", "capability", "org.pangrova.protocol.v2.CapabilityRef", "EI_EXPOSE_REP"},
            {"org.pangrova.execution.stage.binding.RetrievalGenerationIntent", "<init>", "java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String,org.pangrova.protocol.v2.CapabilityRef,java.util.List,int", "void", "capability", "org.pangrova.protocol.v2.CapabilityRef", "EI_EXPOSE_REP2"}
        };
        for (String[] c : cases) {
            String sig = SignatureUtil.createMethodSignature(c[2], c[3]);
            int end = sig.indexOf(')');
            String params = sig.substring(1, end), result = sig.substring(end + 1);
            String type = SignatureUtil.createFieldSignature(c[5]);
            check(full, c[0], c[1], params, result, c[4], type, c[6], true);
            check(full, c[0] + "Other", c[1], params, result, c[4], type, c[6], false);
            check(full, c[0], c[1] + "Other", params, result, c[4], type, c[6], false);
            check(full, c[0], c[1], params + "I", result, c[4], type, c[6], false);
            check(full, c[0], c[1], params, "[B", c[4], type, c[6], false);
            check(full, c[0], c[1], params, result, "otherField", type, c[6], false);
            check(full, c[0], c[1], params, result, c[4], "[B", c[6], false);
            check(full, c[0], c[1], params, result, c[4], type, "HARD_CODE_KEY", false);
            check(full, c[0], c[1], params, result, c[4], type,
                    c[6].equals("EI_EXPOSE_REP") ? "EI_EXPOSE_REP2" : "EI_EXPOSE_REP", false);
        }
        String rerank = "org.pangrova.execution.stage.binding.RerankIntent";
        check(full, rerank, "modelSignature", "", "[B", "modelSignature", "[B", "EI_EXPOSE_REP", false);
        check(full, "org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RerankIntent",
                "modelLimits", "", "Lorg/pangrova/protocol/v2/CapabilityLimits;", "modelLimits",
                "Lorg/pangrova/protocol/v2/CapabilityLimits;", "EI_EXPOSE_REP", false);
    }

    private static void verifyParserBorrowing(String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        String tree = "Lorg/antlr/v4/runtime/tree/ParseTree;";
        String tokens = "Lorg/antlr/v4/runtime/CommonTokenStream;";
        String[][] cases = {
            {"org.pangrova.parser.diagnostic.RecoveryParseDetails", "partialTree", "", tree,
                "partialTree", tree, "EI_EXPOSE_REP"},
            {"org.pangrova.parser.diagnostic.RecoveryParseDetails", "<init>",
                "Lorg/pangrova/parser/api/parse/ParseResult;" + tree, "V", "partialTree", tree, "EI_EXPOSE_REP2"},
            {"org.pangrova.parser.build.AstBuilder", "<init>",
                tokens + "Lorg/pangrova/parser/api/parse/SqlDialect;", "V", "tokenStream", tokens, "EI_EXPOSE_REP2"}
        };
        for (String[] c : cases) {
            check(full, c[0], c[1], c[2], c[3], c[4], c[5], c[6], true);
            check(full, c[0] + "Other", c[1], c[2], c[3], c[4], c[5], c[6], false);
            check(full, c[0], c[1] + "Other", c[2], c[3], c[4], c[5], c[6], false);
            check(full, c[0], c[1], c[2] + "I", c[3], c[4], c[5], c[6], false);
            check(full, c[0], c[1], c[2], "[B", c[4], c[5], c[6], false);
            check(full, c[0], c[1], c[2], c[3], "otherField", c[5], c[6], false);
            check(full, c[0], c[1], c[2], c[3], c[4], "[B", c[6], false);
            check(full, c[0], c[1], c[2], c[3], c[4], c[5], "HARD_CODE_KEY", false);
            check(full, c[0], c[1], c[2], c[3], c[4], c[5],
                    c[6].equals("EI_EXPOSE_REP") ? "EI_EXPOSE_REP2" : "EI_EXPOSE_REP", false);
        }
    }

    private static void verifyImmutableResourceGrants(String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        String type = "Lorg/pangrova/protocol/v2/StageResourceGrant;";
        for (String record : new String[]{"LeafTask", "DmlWriteTask"}) {
            String owner = "org.pangrova.execution.binding.TaskRequestFactory$" + record;
            check(full, owner, "resourceGrant", "", type, "resourceGrant", type, "EI_EXPOSE_REP", true);
            check(full, owner + "Other", "resourceGrant", "", type, "resourceGrant", type, "EI_EXPOSE_REP", false);
            check(full, owner, "otherGrant", "", type, "resourceGrant", type, "EI_EXPOSE_REP", false);
            check(full, owner, "resourceGrant", "I", type, "resourceGrant", type, "EI_EXPOSE_REP", false);
            check(full, owner, "resourceGrant", "", "[B", "resourceGrant", type, "EI_EXPOSE_REP", false);
            check(full, owner, "resourceGrant", "", type, "otherGrant", type, "EI_EXPOSE_REP", false);
            check(full, owner, "resourceGrant", "", type, "resourceGrant", "[B", "EI_EXPOSE_REP", false);
            check(full, owner, "resourceGrant", "", type, "resourceGrant", type, "EI_EXPOSE_REP2", false);
            check(full, owner, "resourceGrant", "", type, "resourceGrant", type, "HARD_CODE_KEY", false);
        }
        check(full, "org.pangrova.coordinator.query.execution.dispatch.dag.TaskRequestFactory$LeafTask",
                "resourceGrant", "", type, "resourceGrant", type, "EI_EXPOSE_REP", false);
    }

    private static void verifyManagedSourceBorrowed(String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        String owner = "org.pangrova.persistence.catalog.postgresql.source.JdbcManagedSourceCatalog";
        String oldParams = "Ljavax/sql/DataSource;"
                + "Lorg/pangrova/connector/api/metadata/SourceCatalogDeployment;"
                + "Ljava/util/function/Function;";
        String previousParams = oldParams + "Lorg/pangrova/persistence/jdbc/api/query/ExecutionTransactionFence;";
        String modelParams = previousParams + "Lorg/pangrova/persistence/jdbc/api/security/model/CurrentModelAuthority;";
        String params = modelParams + "Lorg/pangrova/persistence/workflow/api/security/TableControlWriteAuthority;";
        String sourceType = "Ljavax/sql/DataSource;";
        check(full, owner, "<init>", params, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", true);
        check(full, owner, "<init>", modelParams, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", oldParams, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", previousParams, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", params + "I", "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner + "Other", "<init>", params, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "other", params, "V", "dataSource", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", params, "V", "executionFence", sourceType, "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", params, "V", "dataSource", "[B", "EI_EXPOSE_REP2", false);
        check(full, owner, "<init>", params, "V", "dataSource", sourceType, "EI_EXPOSE_REP", false);
    }

    private static void verifyIndexingBorrowed(String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        String manager = "org.pangrova.indexing.storage.IndexStorageManager";
        String context = "org.pangrova.indexing.build.IndexBuildContext";
        String evaluation = "org.pangrova.indexing.evaluation.IndexEvaluationService";
        String cached = "org.pangrova.indexing.builtin.bloom.data.CachedBloomFilterIndexData";
        String bloom = "org.pangrova.common.bloom.SplitBlockBloomFilter";
        verifyIndexingMatch(full, context, "storageManager", "", manager,
                "storageManager", manager, "EI_EXPOSE_REP");
        verifyIndexingMatch(full, context, "<init>",
                "org.pangrova.indexing.config.IndexingConfig,org.pangrova.indexing.strategy.IndexEngineRegistry,"
                + manager + ",org.pangrova.indexing.codec.data.IndexSerializer,"
                + "org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository,"
                + "org.pangrova.indexing.build.GenerationChecker,"
                + "org.pangrova.connector.api.index.SnapshotPinProvider,java.util.concurrent.ExecutorService",
                "void", "storageManager", manager, "EI_EXPOSE_REP2");
        String evaluationParams = "org.pangrova.indexing.strategy.IndexEngineRegistry,"
                + "org.pangrova.indexing.storage.IndexCache," + manager + ","
                + "org.pangrova.indexing.codec.data.IndexSerializer,"
                + "org.pangrova.indexing.evaluation.IndexEvalCircuitBreaker";
        verifyIndexingMatch(full, evaluation, "<init>", evaluationParams, "void",
                "storageManager", manager, "EI_EXPOSE_REP2");
        verifyIndexingMatch(full, evaluation, "<init>", evaluationParams, "void",
                "cache", "org.pangrova.indexing.storage.IndexCache", "EI_EXPOSE_REP2");
        verifyIndexingMatch(full, cached, "filter", "", bloom,
                "deserializedFilter", bloom, "EI_EXPOSE_REP");
        verifyIndexingMatch(full, cached, "<init>",
                "org.pangrova.indexing.api.data.BloomFilterIndexData," + bloom,
                "void", "deserializedFilter", bloom, "EI_EXPOSE_REP2");
    }

    private static void verifyIndexingMatch(Filter full, String owner, String method,
            String params, String result, String field, String type, String bug) {
        String signature = SignatureUtil.createMethodSignature(params, result);
        int end = signature.indexOf(')');
        String arguments = signature.substring(1, end);
        String returnType = signature.substring(end + 1);
        String fieldType = SignatureUtil.createFieldSignature(type);
        check(full, owner, method, arguments, returnType, field, fieldType, bug, true);
        check(full, owner + "Other", method, arguments, returnType, field, fieldType, bug, false);
        check(full, owner, method + "Other", arguments, returnType, field, fieldType, bug, false);
        check(full, owner, method, arguments, returnType, field + "Other", fieldType, bug, false);
        check(full, owner, method, arguments, returnType, field, "Ljava/lang/Object;", bug, false);
        check(full, owner, method, arguments, returnType, field, fieldType,
                "NP_BOOLEAN_RETURN_NULL", false);
    }

    private static void verifyImmutableAccessors(String manifest, String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(manifest))) {
            String[] part = line.split(java.util.regex.Pattern.quote("|"), -1);
            Filter isolated = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2], field = part[5];
            String signature = SignatureUtil.createMethodSignature(part[3], part[4]);
            int end = signature.indexOf(')');
            String params = signature.substring(1, end), result = signature.substring(end + 1);
            String type = SignatureUtil.createFieldSignature(part[6]);
            for (Filter filter : new Filter[]{isolated, full}) {
                check(filter, owner, method, params, result, field, type, "EI_EXPOSE_REP", true);
                check(filter, owner + "Other", method, params, result, field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method + "Other", params, result, field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, "I", result, field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, "Ljava/lang/Object;", result, field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, "[B", field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, "Ljava/lang/Object;", field, type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field + "Other", type, "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, "[B", "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, "Ljava/lang/Object;", "EI_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, type, "EI_EXPOSE_REP2", false);
                check(filter, owner, method, params, result, field, type, "MS_EXPOSE_REP", false);
                check(filter, owner, method, params, result, field, type, "NP_BOOLEAN_RETURN_NULL", false);
            }
        }
        String prepared = "org.pangrova.client.jdbc.PangrovaPreparedStatement";
        check(full, prepared, "getConnection", "", "Ljava/sql/Connection;", "connection",
                "Lorg/pangrova/client/jdbc/PangrovaConnection;", "EI_EXPOSE_REP", false);
        check(full, prepared, "getResultSet", "", "Ljava/sql/ResultSet;", "currentResultSet",
                "Ljava/sql/ResultSet;", "EI_EXPOSE_REP", false);
    }

    private static void verifyJdbcForwards(String manifest, String fullFilterPath) throws Exception {
        Filter full = Filter.parseFilter(fullFilterPath);
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(manifest))) {
            String[] part = line.split("\\\\|", -1);
            Filter isolated = Filter.parseFilter(part[0]);
            String owner = part[1], method = part[2], params = part[3], result = part[4];
            String signature = SignatureUtil.createMethodSignature(params, result);
            command(isolated, owner, method, signature, "SQL_INJECTION_JDBC", true);
            command(full, owner, method, signature, "SQL_INJECTION_JDBC", true);
            // Client JDBC intentionally has several approved overloads; the Artifact methods have one each.
            var boundaries = owner.startsWith("org.pangrova.generation.")
                    ? java.util.List.of(isolated, full) : java.util.List.of(isolated);
            for (Filter boundary : boundaries) {
                command(boundary, owner + "Other", method, signature, "SQL_INJECTION_JDBC", false);
                command(boundary, owner, method + "Other", signature, "SQL_INJECTION_JDBC", false);
                command(boundary, owner, method, SignatureUtil.createMethodSignature(params + ",int", result),
                        "SQL_INJECTION_JDBC", false);
                command(boundary, owner, method, SignatureUtil.createMethodSignature("java.lang.String", result),
                        "SQL_INJECTION_JDBC", false);
                command(boundary, owner, method, SignatureUtil.createMethodSignature(params, "java.lang.Object"),
                        "SQL_INJECTION_JDBC", false);
                command(boundary, owner, method, signature, "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", false);
                command(boundary, owner, method, signature, "NP_BOOLEAN_RETURN_NULL", false);
            }
        }
        String prepared = "org.pangrova.client.jdbc.PangrovaPreparedStatement";
        command(full, prepared, "executeQuery", "()Ljava/sql/ResultSet;", "SQL_INJECTION_JDBC", false);
        command(full, "org.pangrova.client.jdbc.PangrovaStatement", "classifyTopLevelKeyword",
                "(Ljava/lang/String;I)Ljava/lang/Boolean;", "NP_BOOLEAN_RETURN_NULL", false);
    }

    private static void command(Filter filter, String owner, String method, String signature,
            String bug, boolean expected) {
        BugInstance finding = new BugInstance(bug, 2).addClass(owner).addMethod(owner, method, signature, false);
        if (filter.match(finding) != expected) {
            throw new AssertionError("Unexpected runner classification: " + owner + "." + method
                    + signature + " bug=" + bug);
        }
    }
}
'''


def main():
    properties = ET.parse(ROOT / 'pom.xml').find('m:properties', NS)
    version = properties.find('m:spotbugs.version', NS).text
    repository = Path(os.environ.get('MAVEN_REPO_LOCAL', str(Path.home() / '.m2/repository')))
    engine = repository / 'com/github/spotbugs/spotbugs' / version
    security_version = properties.find('m:findsecbugs.version', NS).text
    security_plugin = repository / 'com/h3xstream/findsecbugs/findsecbugs-plugin' / security_version \
        / f'findsecbugs-plugin-{security_version}.jar'
    if not security_plugin.is_file():
        raise RuntimeError(f'Missing pinned FindSecBugs plugin: {security_plugin}')
    plugin_version = properties.find('m:spotbugs-maven-plugin.version', NS).text
    plugin = ET.parse(repository / 'com/github/spotbugs/spotbugs-maven-plugin' / plugin_version
                      / f'spotbugs-maven-plugin-{plugin_version}.pom')
    plugin_properties = plugin.find('m:properties', NS)
    overrides = {}
    for dependency in plugin.findall('m:dependencies/m:dependency', NS):
        declared = dependency.find('m:version', NS)
        if declared is None:
            continue
        selected = declared.text
        if selected.startswith('${'):
            property_value = plugin_properties.find('m:' + selected[2:-1], NS)
            if property_value is None:
                continue
            selected = property_value.text
        overrides[(dependency.find('m:groupId', NS).text,
                   dependency.find('m:artifactId', NS).text)] = selected
    dependencies = [engine / f'spotbugs-{version}.jar']
    pom_files = [engine / f'spotbugs-{version}.pom',
                 repository / 'com/github/spotbugs/spotbugs-annotations' / version
                 / f'spotbugs-annotations-{version}.pom']
    for pom in pom_files:
        for dependency in ET.parse(pom).findall('m:dependencies/m:dependency', NS):
            group = dependency.find('m:groupId', NS).text
            artifact = dependency.find('m:artifactId', NS).text
            dependency_version = overrides.get((group, artifact), dependency.find('m:version', NS).text)
            dependencies.append(repository / group.replace('.', '/') / artifact / dependency_version
                                / f'{artifact}-{dependency_version}.jar')
    logger_version = overrides[('org.slf4j', 'slf4j-simple')]
    dependencies.append(repository / 'org/slf4j/slf4j-simple' / logger_version
                        / f'slf4j-simple-{logger_version}.jar')
    for dependency in dependencies:
        if not dependency.is_file():
            raise RuntimeError(f'Missing pinned SpotBugs dependency: {dependency}; run static-analysis first')
    java_home = os.environ.get('JAVA_HOME')
    javac = str(Path(java_home) / 'bin/javac') if java_home else 'javac'
    java = str(Path(java_home) / 'bin/java') if java_home else 'java'
    with tempfile.TemporaryDirectory(prefix='pangrova-immutable-filter-') as directory:
        temporary = Path(directory)
        # The distribution JAR's manifest names sibling files absent from Maven's
        # group/artifact layout. Use its unchanged classes/resources as a directory,
        # and the explicitly resolved dependencies, rather than suppress path lint.
        engine_classes = temporary / 'engine'
        with zipfile.ZipFile(dependencies[0]) as archive:
            archive.extractall(engine_classes)
        classpath = os.pathsep.join(map(str, [engine_classes, *dependencies[1:]]))
        full_filter = ET.parse(ROOT / 'config/spotbugs/exclude-filter.xml')
        matches = [match for match in full_filter.getroot()
                   if any(child.tag.endswith('Method') and child.get('name') == 'declarationSha256'
                          for child in match)
                   and any(child.tag.endswith('Field') and child.get('name') == 'declarationSha256'
                           for child in match)]
        if len(matches) != 1:
            raise RuntimeError('Expected one exact immutable hash classification')
        isolated = ET.Element('FindBugsFilter')
        isolated.append(deepcopy(matches[0]))
        for element in isolated.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        classification = temporary / 'classification.xml'
        ET.ElementTree(isolated).write(classification, encoding='utf-8', xml_declaration=True)
        runner_matches = [match for match in full_filter.getroot()
                          if any(child.tag.endswith('Class') and child.get('name') ==
                                 'org.pangrova.testing.testkit.security.TestTlsAuthority' for child in match)
                          and any(child.tag.endswith('Method') and child.get('name') == 'run' for child in match)]
        if len(runner_matches) != 1:
            raise RuntimeError('Expected one exact keytool runner classification')
        runner_filter = ET.Element('FindBugsFilter')
        runner_filter.append(deepcopy(runner_matches[0]))
        for element in runner_filter.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        runner_classification = temporary / 'runner-classification.xml'
        ET.ElementTree(runner_filter).write(runner_classification, encoding='utf-8', xml_declaration=True)
        jitter_matches = [match for match in full_filter.getroot()
                          if any(child.tag.endswith('Class') and child.get('name') ==
                                 'org.pangrova.common.client.session.RetryPolicy' for child in match)
                          and any(child.tag.endswith('Method') and child.get('name') ==
                                  'delayForAttemptWithJitter' for child in match)]
        if len(jitter_matches) != 1:
            raise RuntimeError('Expected one exact non-security retry jitter classification')
        jitter_filter = ET.Element('FindBugsFilter')
        jitter_filter.append(deepcopy(jitter_matches[0]))
        for element in jitter_filter.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        jitter_classification = temporary / 'jitter-classification.xml'
        ET.ElementTree(jitter_filter).write(jitter_classification, encoding='utf-8', xml_declaration=True)
        paimon = 'org.pangrova.connector.paimon.'
        catalog = 'org.apache.paimon.catalog.Catalog'
        splits = paimon + 'PaimonSplitRegistry'
        table = 'org.apache.paimon.table.Table'
        io = 'org.apache.paimon.fs.FileIO'
        metadata_params = ','.join((catalog, 'java.lang.String', splits))
        split_params = ','.join((catalog, splits))
        borrowed_constructors = [
            ('PaimonIncrementalScanCapability', catalog, 'catalog', catalog),
            ('PaimonMetadata', metadata_params, 'catalog', catalog),
            ('PaimonMetadata', metadata_params, 'splitRegistry', splits),
            ('PaimonPageSourceProvider', splits, 'splitRegistry', splits),
            ('PaimonSplitManager', split_params, 'catalog', catalog),
            ('PaimonSplitManager', split_params, 'splitRegistry', splits),
            ('write.PaimonPageSinkProvider', catalog + ',java.lang.String', 'catalog', catalog),
            ('fs.PaimonFileSystemAccessor', io, 'fileIO', io),
            ('fs.PaimonColumnValueReader', table, 'table', table),
        ]
        borrowed_constructors += [
            ('system.Paimon' + kind + 'SystemTable', table, 'table', table)
            for kind in ('AuditLog', 'Branches', 'Consumers', 'Manifests', 'Options', 'Schemas', 'Snapshots', 'Tags')
        ]
        expected_paimon = {(paimon + owner, '<init>', params, 'void', name, field_type, 'EI_EXPOSE_REP2')
                           for owner, params, name, field_type in borrowed_constructors}
        expected_paimon.add((paimon + 'PaimonConnector', 'getMetadata',
                             'org.pangrova.connector.api.session.ConnectorSession',
                             'org.pangrova.connector.api.metadata.ConnectorMetadata', 'metadata',
                             paimon + 'PaimonMetadata', 'EI_EXPOSE_REP'))
        borrowed_matches = []
        actual_paimon = set()
        for match in full_filter.getroot():
            fields = {child.tag.rsplit('}', 1)[-1]: child.attrib for child in match}
            if not fields.get('Class', {}).get('name', '').startswith(paimon):
                continue
            if fields.get('Bug', {}).get('pattern') not in ('EI_EXPOSE_REP', 'EI_EXPOSE_REP2'):
                continue
            if len(match) != 4 or set(fields) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Paimon borrowed-service classifications require an exact method and field')
            method, field = fields['Method'], fields['Field']
            case = (fields['Class']['name'], method['name'], method['params'], method['returns'],
                    field['name'], field['type'], fields['Bug']['pattern'])
            if case in actual_paimon or case not in expected_paimon:
                raise RuntimeError(f'Unexpected or duplicate Paimon borrowed-service classification: {case}')
            actual_paimon.add(case)
            borrowed_matches.append(match)
        if expected_paimon != actual_paimon:
            raise RuntimeError(f'Missing Paimon borrowed-service classifications: {expected_paimon - actual_paimon}')
        deployment = 'org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository'
        dispatcher = 'org.pangrova.metadata.event.MetadataEventDispatcher'
        cache = 'org.pangrova.metadata.api.cache.MetadataCache'
        registry_params = ','.join(('org.pangrova.persistence.table.api.provider.TableStores',
                                    deployment, 'org.pangrova.metadata.api.catalog.ConnectorHandleFactory',
                                    'org.pangrova.metadata.api.catalog.FederationHandleFactory', dispatcher))
        sync_params = ','.join(('org.pangrova.persistence.catalog.api.projection.TableProjectionRepository',
                               'org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions',
                               'org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository',
                               cache, dispatcher))
        registry = 'org.pangrova.metadata.catalog.registry.CatalogRegistry'
        sync = 'org.pangrova.metadata.outbox.MetadataSyncService'
        support = 'org.pangrova.persistence.catalog.postgresql.transaction.PostgresCatalogPersistenceSupport'
        expected_projection_di = {
            (registry, registry_params, 'store', deployment),
            (registry, registry_params, 'eventDispatcher', dispatcher),
            (sync, sync_params, 'cache', cache),
            (sync, sync_params, 'eventDispatcher', dispatcher),
            (support, 'javax.sql.DataSource', 'dataSource', 'javax.sql.DataSource'),
        }
        projection_matches = [match for match in full_filter.getroot()
                              if any(child.tag.endswith('Class') and child.get('name') in
                                     {registry, sync, support} for child in match)
                              and any(child.tag.endswith('Bug') and child.get('pattern') ==
                                      'EI_EXPOSE_REP2' for child in match)]
        actual_projection_di = set()
        for match in projection_matches:
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Catalog projection ownership classification is not exact')
            owner, method, field = (elements[key] for key in ('Class', 'Method', 'Field'))
            case = (owner.attrib['name'], method.attrib['params'], field.attrib['name'], field.attrib['type'])
            if (case not in expected_projection_di or case in actual_projection_di
                    or method.attrib['name'] != '<init>' or method.attrib['returns'] != 'void'):
                raise RuntimeError(f'Unexpected or duplicate catalog projection classification: {case}')
            actual_projection_di.add(case)
        if actual_projection_di != expected_projection_di:
            raise RuntimeError('Missing catalog projection borrowed-ownership classifications')
        borrowed_matches += projection_matches
        borrowed_cases = []
        for index, match in enumerate(borrowed_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Borrowed service classification is not exact')
            owner, method, field, bug = (elements[key] for key in ('Class', 'Method', 'Field', 'Bug'))
            isolated_borrowed = ET.Element('FindBugsFilter')
            isolated_borrowed.append(deepcopy(match))
            for element in isolated_borrowed.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'borrowed-{index}.xml'
            ET.ElementTree(isolated_borrowed).write(classification_file, encoding='utf-8', xml_declaration=True)
            borrowed_cases.append('|'.join([str(classification_file), owner.attrib['name'], method.attrib['name'],
                                           method.attrib['params'], method.attrib['returns'], field.attrib['name'],
                                           field.attrib['type'], bug.attrib['pattern']]))
        borrowed_manifest = temporary / 'borrowed-cases.txt'
        borrowed_manifest.write_text('\n'.join(borrowed_cases), encoding='utf-8')
        jdbc_matches = [match for match in full_filter.getroot()
                        if any(child.tag.endswith('Class') and child.get('name', '').startswith(
                            ('org.pangrova.client.jdbc.', 'org.pangrova.persistence.generation.postgresql.')) for child in match)
                        and any(child.tag.endswith('Bug') and child.get('pattern') == 'SQL_INJECTION_JDBC'
                                for child in match)]
        expected_jdbc = {
            ('org.pangrova.client.jdbc.PangrovaConnection', 'prepareStatement', params, 'java.sql.PreparedStatement')
            for params in ('java.lang.String,int', 'java.lang.String,int,int', 'java.lang.String,int,int,int',
                           'java.lang.String,int[]', 'java.lang.String,java.lang.String[]')
        } | {
            ('org.pangrova.client.jdbc.PangrovaStatement', method, params, result)
            for method, result in (('execute', 'boolean'), ('executeUpdate', 'int'))
            for params in ('java.lang.String,int', 'java.lang.String,int[]', 'java.lang.String,java.lang.String[]')
        }
        expected_jdbc |= {
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationOutputQueries', 'loadOutputsForPages', 'java.sql.Connection,java.lang.String,java.lang.String,java.util.List,java.lang.String', 'java.util.List'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationOutputQueries', 'loadPublishedSourceSplits', 'java.sql.Connection,java.lang.String,java.lang.String,java.util.List', 'java.util.Map'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationOutputQueries', 'loadSourceOutputs', 'java.sql.Connection,java.lang.String,java.lang.String,java.util.List,java.util.List', 'void'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationOutputQueries', 'loadStoredSplits', 'java.sql.Connection,java.lang.String,java.lang.String,java.util.List,java.util.Map', 'void'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationOutputQueries', 'loadSucceededPages', 'java.sql.Connection,java.lang.String,java.lang.String,java.util.List,java.lang.String', 'java.util.Set'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection', 'findPage', 'java.sql.Connection,java.lang.String,java.lang.String,java.lang.String,boolean', 'java.util.Optional'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection', 'selectClaimablePage', 'java.sql.Connection,org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection$Selection', 'java.util.Optional'),
            ('org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStoreMappings', 'findJob', 'java.sql.Connection,java.lang.String,java.lang.String,boolean', 'java.util.Optional'),
            ('org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore', 'findEvidence', 'java.sql.Connection,org.pangrova.generation.api.identity.GenerationJobKey,java.lang.String,boolean,boolean', 'java.util.Optional'),
            ('org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore', 'insertEvidence', 'java.sql.Connection,org.pangrova.generation.api.identity.GenerationJobKey,java.lang.String,byte[],java.lang.String,boolean', 'void'),
        }
        if len(jdbc_matches) != len(expected_jdbc):
            raise RuntimeError('Expected 21 exact JDBC forwarding/Artifact SQL classifications')
        jdbc_cases = []
        actual_jdbc = set()
        for index, match in enumerate(jdbc_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 3 or set(elements) != {'Class', 'Method', 'Bug'}:
                raise RuntimeError('JDBC forwarding classification is not a flat exact Match')
            owner, method = elements['Class'], elements['Method']
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['params'], method.attrib['returns'])
            if case not in expected_jdbc or case in actual_jdbc:
                raise RuntimeError(f'Unexpected or duplicate JDBC classification: {case}')
            actual_jdbc.add(case)
            isolated_jdbc = ET.Element('FindBugsFilter')
            isolated_jdbc.append(deepcopy(match))
            for element in isolated_jdbc.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'jdbc-{index}.xml'
            ET.ElementTree(isolated_jdbc).write(classification_file, encoding='utf-8', xml_declaration=True)
            jdbc_cases.append('|'.join([str(classification_file), *case]))
        jdbc_manifest = temporary / 'jdbc-cases.txt'
        jdbc_manifest.write_text('\n'.join(jdbc_cases), encoding='utf-8')
        identity_matches = [match for match in full_filter.getroot()
                            if any(child.tag.endswith('Class') and child.get('name', '').startswith(
                                'org.pangrova.client.jdbc.') for child in match)
                            and any(child.tag.endswith('Bug') and child.get('pattern') == 'EI_EXPOSE_REP'
                                    for child in match)]
        expected_identities = {
            ('org.pangrova.client.jdbc.PangrovaDatabaseMetaData', 'getConnection', '', 'java.sql.Connection',
             'connection', 'org.pangrova.client.jdbc.PangrovaConnection'),
            ('org.pangrova.client.jdbc.PangrovaResultSet', 'getStatement', '', 'java.sql.Statement',
             'owningStatement', 'java.sql.Statement'),
            ('org.pangrova.client.jdbc.PangrovaStatement', 'getConnection', '', 'java.sql.Connection',
             'connection', 'org.pangrova.client.jdbc.PangrovaConnection'),
            ('org.pangrova.client.jdbc.PangrovaStatement', 'getResultSet', '', 'java.sql.ResultSet',
             'currentResultSet', 'java.sql.ResultSet'),
        }
        if len(identity_matches) != len(expected_identities):
            raise RuntimeError('Expected four exact JDBC identity getter classifications')
        identity_cases = []
        actual_identities = set()
        for index, match in enumerate(identity_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('JDBC identity classification is not a flat exact Match')
            owner, method, field = (elements[key] for key in ('Class', 'Method', 'Field'))
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['params'], method.attrib['returns'],
                    field.attrib['name'], field.attrib['type'])
            if case not in expected_identities or case in actual_identities:
                raise RuntimeError(f'Unexpected or duplicate JDBC identity classification: {case}')
            actual_identities.add(case)
            isolated_identity = ET.Element('FindBugsFilter')
            isolated_identity.append(deepcopy(match))
            for element in isolated_identity.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'jdbc-identity-{index}.xml'
            ET.ElementTree(isolated_identity).write(classification_file, encoding='utf-8', xml_declaration=True)
            identity_cases.append('|'.join([str(classification_file), *case]))
        identity_manifest = temporary / 'jdbc-identity-cases.txt'
        identity_manifest.write_text('\n'.join(identity_cases), encoding='utf-8')
        expected_metadata = {
            ('org.pangrova.generation.api.exchange.GenerationExchangeFinalization', 'partitions', 'java.util.List'),
            ('org.pangrova.generation.api.exchange.GenerationExchangeRegistration', 'endpoints', 'java.util.List'),
            ('org.pangrova.generation.api.graph.GenerationStageGraph', 'stages', 'java.util.List'),
            ('org.pangrova.generation.api.graph.GenerationStageGraph', 'exchanges', 'java.util.List'),
            ('org.pangrova.generation.api.graph.GenerationStageGraph', 'modelSnapshots', 'java.util.List'),
            ('org.pangrova.generation.api.graph.template.GenerationStageTemplate', 'publicationReads', 'java.util.List'),
            ('org.pangrova.generation.api.dependency.GenerationDependencySet', 'dependencies', 'java.util.List'),
            ('org.pangrova.metadata.api.object.TableMetadata', 'columns', 'java.util.List'),
            ('org.pangrova.metadata.api.object.TableMetadata', 'partitionSpec', 'java.util.Map'),
            ('org.pangrova.metadata.api.object.TableMetadata', 'sortOrder', 'java.util.List'),
            ('org.pangrova.metadata.api.object.TableMetadata', 'properties', 'java.util.Map'),
        }
        metadata_owners = {owner for owner, _, _ in expected_metadata}
        metadata_matches = [match for match in full_filter.getroot()
                            if any(child.tag.endswith('Class') and child.get('name') in metadata_owners
                                   for child in match)
                            and any(child.tag.endswith('Bug') and child.get('pattern') == 'EI_EXPOSE_REP'
                                    for child in match)]
        if len(metadata_matches) != len(expected_metadata):
            raise RuntimeError('Expected 11 exact immutable metadata collection classifications')
        metadata_cases = []
        actual_metadata = set()
        for index, match in enumerate(metadata_matches):
            elements = {child.tag.rsplit('}', 1)[-1]: child for child in match}
            if len(match) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
                raise RuntimeError('Metadata collection classification is not a flat exact Match')
            owner, method, field = (elements[key] for key in ('Class', 'Method', 'Field'))
            case = (owner.attrib['name'], method.attrib['name'], method.attrib['returns'])
            if (case not in expected_metadata or case in actual_metadata or method.attrib['params'] != ''
                    or field.attrib['name'] != method.attrib['name']
                    or field.attrib['type'] != method.attrib['returns']):
                raise RuntimeError(f'Unexpected or duplicate metadata collection classification: {case}')
            actual_metadata.add(case)
            isolated_metadata = ET.Element('FindBugsFilter')
            isolated_metadata.append(deepcopy(match))
            for element in isolated_metadata.iter():
                element.tag = element.tag.rsplit('}', 1)[-1]
            classification_file = temporary / f'metadata-collection-{index}.xml'
            ET.ElementTree(isolated_metadata).write(classification_file, encoding='utf-8', xml_declaration=True)
            metadata_cases.append('|'.join([str(classification_file), case[0], case[1], '', case[2],
                                           field.attrib['name'], field.attrib['type']]))
        metadata_manifest = temporary / 'metadata-collection-cases.txt'
        metadata_manifest.write_text('\n'.join(metadata_cases), encoding='utf-8')
        vector_owner = 'org.pangrova.planner.type.StorageVectorRelDataType'
        vector_matches = [match for match in full_filter.getroot()
                          if any(child.tag.endswith('Class') and child.get('name') == vector_owner
                                 for child in match)]
        if len(vector_matches) != 1:
            raise RuntimeError('Expected one exact immutable canonical VECTOR declaration classification')
        elements = {child.tag.rsplit('}', 1)[-1]: child for child in vector_matches[0]}
        if len(vector_matches[0]) != 4 or set(elements) != {'Class', 'Method', 'Field', 'Bug'}:
            raise RuntimeError('VECTOR declaration classification is not a flat exact Match')
        if (elements['Method'].attrib != {'name': 'logicalType', 'params': '',
                                        'returns': 'org.pangrova.protocol.v2.LogicalType'}
                or elements['Field'].attrib != {'name': 'logicalType',
                                               'type': 'org.pangrova.protocol.v2.LogicalType'}
                or elements['Bug'].attrib != {'pattern': 'EI_EXPOSE_REP'}):
            raise RuntimeError('VECTOR declaration classification changed its immutable boundary')
        vector_filter = ET.Element('FindBugsFilter')
        vector_filter.append(deepcopy(vector_matches[0]))
        for element in vector_filter.iter():
            element.tag = element.tag.rsplit('}', 1)[-1]
        vector_classification = temporary / 'vector-declaration.xml'
        ET.ElementTree(vector_filter).write(vector_classification, encoding='utf-8', xml_declaration=True)
        vector_manifest = temporary / 'vector-declaration-cases.txt'
        vector_manifest.write_text('|'.join([str(vector_classification), vector_owner, 'logicalType', '',
                                           'org.pangrova.protocol.v2.LogicalType', 'logicalType',
                                           'org.pangrova.protocol.v2.LogicalType']), encoding='utf-8')
        source = temporary / 'ImmutableFilterBoundary.java'
        source.write_text(SOURCE, encoding='utf-8')
        subprocess.run([javac, '-Xlint:all', '-Werror', '-cp', classpath,
                        '-d', str(temporary), str(source)], check=True)
        subprocess.run([java, '-cp', classpath + os.pathsep + str(temporary),
                        'ImmutableFilterBoundary', str(classification), str(runner_classification),
                        str(security_plugin), str(jitter_classification), str(borrowed_manifest),
                        str(jdbc_manifest), str(ROOT / 'config/spotbugs/exclude-filter.xml'),
                        str(identity_manifest), str(metadata_manifest), str(vector_manifest)], check=True)
    print('PASS: exact immutable hash classified; this Match rejects 8 changed-type/method/field/class/bug cases.')
    print('PASS: exact keytool runner classified; this Match rejects 7 class/method/signature/bug changes.')
    print('PASS: exact non-security retry jitter classified; this Match rejects 7 owner/method/signature/bug changes.')
    print('PASS: Exact Paimon borrowed-service declarations verified; each Match rejects 9 signature/field/type/bug changes.')
    print('PASS: five migrated catalog ownership classifications match current constructors and reject 9 changed boundaries.')
    print('PASS: 21 exact JDBC forwarding/Artifact methods classified; each rejects 7 changed boundaries, Artifact also in full filter.')
    print('PASS: complete filter does not exclude PreparedStatement SQL construction or Boolean-null finding.')
    print('PASS: four JDBC identity getters classified; isolated/full filters each reject 12 changed boundaries.')
    print('PASS: complete filter does not exclude obsolete PreparedStatement identity getter findings.')
    print('PASS: 11 immutable metadata collection getters classified; isolated/full filters each reject 12 changed boundaries.')
    print('PASS: canonical immutable VECTOR getter classified; isolated/full filters reject 12 changed boundaries.')
    print('PASS: six exact indexing ownership classifications each reject changed owner/method/field/type/bug.')
    print('PASS: managed-source DataSource classification matches only the new constructor; old and altered boundaries remain checked.')
    print('PASS: two immutable StageResourceGrant getters reject 8 changed boundaries each and the retired owner.')


if __name__ == '__main__':
    main()
