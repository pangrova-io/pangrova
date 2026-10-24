package org.pangrova.coordinator.query.execution;

import io.substrait.proto.NamedStruct;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier.ObservedPlan;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.authz.DeferredAuthorizationScope.ReadMode;
import org.pangrova.security.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.authz.Privilege;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Actual policy observations and original SQL actions from the single dispatched compilation. */
public record WritePolicyObservation(List<TablePermission> reads, Set<Privilege> writePrivileges,
                                     WriteDeclarationSnapshot declaration) {
    public WritePolicyObservation {
        reads = List.copyOf(reads);
        writePrivileges = Set.copyOf(writePrivileges);
        java.util.Objects.requireNonNull(declaration, "declaration");
        if (writePrivileges.isEmpty()) {
            throw new IllegalArgumentException("Original write actions are required");
        }
    }

    static WritePolicyObservation capture(ObservedPlan observed, SqlCompiler compiler, Statement statement) {
        var schemas = AuthorizedRelationSchemas.extract(observed.plan().serializeValidated(compiler));
        if (!schemas.keySet().equals(observed.policies().keySet())) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Write source policy observations are incomplete");
        }
        var tables = new java.util.HashMap<String, QualifiedTableName>();
        for (var reference : observed.plan().tableReferences()) {
            var table = QualifiedTableName.resolve(reference);
            var previous = tables.putIfAbsent(table.toString(), table);
            if (previous != null && !previous.equals(table)) {
                throw new IllegalArgumentException("Ambiguous compiled source table identity");
            }
        }
        var logicalByPhysical = observed.plan().physicalToLogicalDynamicTables();
        List<TablePermission> reads = new ArrayList<>();
        schemas.forEach((name, bytes) -> {
            try {
                var schema = NamedStruct.parseFrom(bytes);
                var policies = observed.policies().get(name);
                // Policy masks are keyed by top-level SDK columns, unlike flattened nested schema names.
                Set<String> columns = policies.columnMasks().keySet();
                if (columns.isEmpty() || schema.getStruct().getTypesCount() != columns.size()) {
                    throw new org.pangrova.spi.security.AccessDeniedException(
                            "Write read columns lack exact policy evidence");
                }
                var scan = java.util.Objects.requireNonNull(tables.get(name));
                // A rewritten Dynamic Table scan defers to its logical identity, exactly as
                // a query read does; the physical generation is never an authorization subject.
                reads.add(new TablePermission(logicalByPhysical.getOrDefault(scan, scan), columns,
                        Privilege.SELECT, ReadMode.SECURITY_TRANSFORMED, policies));
            } catch (com.google.protobuf.InvalidProtocolBufferException exception) {
                throw new IllegalArgumentException("Compiled write schema is malformed", exception);
            }
        });
        var declaration = observed.plan().dmlWriteTarget().flatMap(org.pangrova.planner.DmlWriteTarget::declaration)
                .orElseThrow(() -> new org.pangrova.spi.security.AccessDeniedException(
                        "Write target lacks its compiled connector declaration"));
        return new WritePolicyObservation(reads, actions(statement), declaration);
    }

    private static Set<Privilege> actions(Statement statement) {
        if (statement instanceof InsertStatement || statement instanceof InsertSelectStatement
                || statement instanceof InsertOverwriteStatement) {
            return Set.of(Privilege.INSERT);
        }
        if (statement instanceof UpdateStatement) return Set.of(Privilege.UPDATE);
        if (statement instanceof DeleteStatement) return Set.of(Privilege.DELETE);
        if (statement instanceof MergeStatement merge) {
            Set<Privilege> actions = new HashSet<>();
            for (var clause : merge.clauses()) {
                actions.add(action(clause));
            }
            return Set.copyOf(actions);
        }
        throw new IllegalArgumentException("Original SQL statement is not a supported relational write");
    }

    private static Privilege action(MergeStatement.MergeClause clause) {
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate) return Privilege.UPDATE;
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedDelete) return Privilege.DELETE;
        if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert) return Privilege.INSERT;
        throw new IllegalArgumentException("Unsupported MERGE action");
    }
}
