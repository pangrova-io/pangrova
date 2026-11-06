package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.persistence.security.api.grant.PrivilegeMutationRepository;
import org.pangrova.persistence.security.postgresql.grant.JdbcPrivilegeMutationRepository;
import org.pangrova.server.coordinator.sql.security.PrivilegeStatementTasks;

import javax.sql.DataSource;
import java.util.Optional;

/** Role-specific composition: privilege mutation never becomes available through authentication bypass. */
final class SecurityAdministrationBootstrap {
    private SecurityAdministrationBootstrap() {}

    static void register(MetadataExecutionFactory factory, CoordinatorBootstrapConfig config, DataSource source) {
        boolean enabled = config.securityEnabled() && config.securityConfig() != null
                && config.securityConfig().authenticatorType()
                        != CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.NOOP && source != null;
        Optional<PrivilegeMutationRepository> repository = enabled
                ? Optional.of(new JdbcPrivilegeMutationRepository(source)) : Optional.empty();
        new PrivilegeStatementTasks(repository).register(factory);
        Optional<org.pangrova.persistence.security.api.tenant.TenantStore> tenants = enabled
                ? Optional.of(new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source))
                : Optional.empty();
        new org.pangrova.server.coordinator.sql.tenant.TenantStatementTasks(tenants).register(factory);
    }
}
