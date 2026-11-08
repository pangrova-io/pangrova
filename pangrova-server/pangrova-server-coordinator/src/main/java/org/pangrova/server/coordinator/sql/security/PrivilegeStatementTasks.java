package org.pangrova.server.coordinator.sql.security;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.query.core.session.SessionTableNames;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.GrantPrivilegeStatement;
import org.pangrova.parser.api.ast.statement.RevokePrivilegeStatement;
import org.pangrova.parser.api.ast.statement.ShowPrivilegeMutationStatement;
import org.pangrova.persistence.security.api.grant.PrivilegeMutationRepository;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.grant.PrivilegeScope;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** SQL boundary only: resolves names and delegates current authorization and atomicity to the injected port. */
public final class PrivilegeStatementTasks {
    private final Optional<PrivilegeMutationRepository> repository;

    /** Empty means explicitly unavailable, never an unsecured or in-memory fallback. */
    public PrivilegeStatementTasks(Optional<PrivilegeMutationRepository> repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    public void register(MetadataExecutionFactory factory) {
        factory.registerTask(GrantPrivilegeStatement.class, this::grant);
        factory.registerTask(ShowPrivilegeMutationStatement.class, this::showReceipt);
        factory.registerTask(RevokePrivilegeStatement.class, this::revoke);
    }

    /** Returns the current privilege mutation receipt to its authenticated owner. */
    public StatementResult showReceipt(ShowPrivilegeMutationStatement statement, StatementExecutionContext context) {
        var session = context.session();
        if (session == null || session.userIdentity() == null || repository.isEmpty()) {
            throw new AuthorizationDeniedException("Authenticated privilege receipt reader is required");
        }
        var receipt = repository.orElseThrow().findReceipt(session.userIdentity(), statement.requestId());
        var row = TypedRow.builder().putString("request_id", statement.requestId().toString())
                .putString("status", receipt.isPresent() ? "CONFIRMED" : "UNCONFIRMED");
        if (receipt.isPresent()) {
            var confirmed = receipt.orElseThrow();
            row.putLong("authorization_revision", confirmed.authorizationRevision())
                    .putInt("changed_grants", confirmed.changedGrants())
                    .putString("recorded_at", confirmed.recordedAt().toString());
        } else {
            row.putRaw("authorization_revision", null).putRaw("changed_grants", null).putRaw("recorded_at", null);
        }
        var result = row.build();
        return new StatementResult(List.of(result), List.copyOf(result.columnNames()), null, StatementCategory.SHOW);
    }

    public StatementResult grant(GrantPrivilegeStatement statement, StatementExecutionContext context) {
        return apply(context, PrivilegeMutation.Operation.GRANT, statement.privileges(), statement.objectType(),
                statement.objectName(), statement.roleNames(), statement.withGrantOption(), false);
    }

    public StatementResult revoke(RevokePrivilegeStatement statement, StatementExecutionContext context) {
        return apply(context, statement.grantOptionOnly() ? PrivilegeMutation.Operation.REVOKE_GRANT_OPTION
                        : PrivilegeMutation.Operation.REVOKE, statement.privileges(), statement.objectType(),
                statement.objectName(), statement.roleNames(), false, statement.cascade());
    }

    private StatementResult apply(StatementExecutionContext context, PrivilegeMutation.Operation operation,
            List<String> names, String kind, Identifier target, List<String> recipients, boolean option,
            boolean cascade) {
        QuerySession session = context.session();
        if (session == null || session.userIdentity() == null || repository.isEmpty()) {
            throw new AuthorizationDeniedException(
                    "SQL privileges require authenticated security and PostgreSQL authority");
        }
        if (session.internalMaterializationExecution()) {
            throw new AuthorizationDeniedException("Internal materialization cannot administer privileges");
        }
        Set<Privilege> privileges = names.stream().map(name -> Privilege.valueOf(name.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toUnmodifiableSet());
        var mutation = new PrivilegeMutation(UUID.fromString(context.queryId()), operation, privileges,
                scope(kind, target, session), Set.copyOf(recipients), option,
                cascade ? PrivilegeMutation.RevokeBehavior.CASCADE : PrivilegeMutation.RevokeBehavior.RESTRICT);
        var receipt = repository.orElseThrow().apply(session.userIdentity(), mutation);
        var row = TypedRow.builder().putString("request_id", receipt.requestId().toString())
                .putLong("authorization_revision", receipt.authorizationRevision())
                .putInt("changed_grants", receipt.changedGrants())
                .putString("recorded_at", receipt.recordedAt().toString()).build();
        return new StatementResult(List.of(row), List.copyOf(row.columnNames()), null, StatementCategory.DDL);
    }

    private static PrivilegeScope scope(String kind, Identifier target, QuerySession session) {
        var type = PrivilegeScope.ObjectType.valueOf(kind.toUpperCase(Locale.ROOT));
        var parts = target.parts();
        if (type == PrivilegeScope.ObjectType.CATALOG || type == PrivilegeScope.ObjectType.STORAGE_PROFILE) {
            if (parts.size() != 1) throw new IllegalArgumentException(kind + " requires one exact name");
            return new PrivilegeScope(type, parts.get(0), null, null);
        }
        if (type == PrivilegeScope.ObjectType.SCHEMA) {
            if (parts.size() == 1) {
                return new PrivilegeScope(type, SessionTableNames.defaultCatalog(session), parts.get(0), null);
            }
            if (parts.size() == 2) return new PrivilegeScope(type, parts.get(0), parts.get(1), null);
            throw new IllegalArgumentException("SCHEMA requires schema or catalog.schema");
        }
        if (parts.isEmpty() || parts.size() > 3) {
            throw new IllegalArgumentException("Object requires 1 to 3 name components");
        }
        var resolved = SessionTableNames.resolveTableRef(parts, session);
        return new PrivilegeScope(type, resolved.catalog(), resolved.schema(), resolved.table());
    }
}
