package org.pangrova.materialization.usecase;

import org.pangrova.materialization.extension.MaterializationExtensionRegistry;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.DefinitionPlanner;
import org.pangrova.materialization.api.port.ExecutionEngine;
import org.pangrova.materialization.api.port.FenceValidator;
import org.pangrova.materialization.api.port.IdGenerator;
import org.pangrova.materialization.api.port.LeaseService;
import org.pangrova.materialization.api.port.MaterializationAuthorizer;
import org.pangrova.persistence.materialization.api.publication.MaterializationCatalog;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PublicationVerifier;
import org.pangrova.materialization.api.port.SourceSnapshotPinner;

/**
 * Complete set of collaborators of the materialization kernel.
 *
 * <p>Grouped so the use-case service has one explicit dependency surface: every
 * port must be supplied by the application wiring and none may be optional.
 *
 * @param repository durable authority
 * @param catalog published read model
 * @param planner definition planner
 * @param execution data-plane executor
 * @param sourcePinning exact source-snapshot holds
 * @param authorizer authorization and capability issuer
 * @param leases execution fence issuer
 * @param fences fence validator
 * @param verifiers connector-specific publication verifier
 * @param extensions explicitly registered refresh strategies and policies
 * @param ids identifier source
 * @param clock time source
 */
public record KernelPorts(
        MaterializationRepository repository,
        MaterializationCatalog catalog,
        DefinitionPlanner planner,
        ExecutionEngine execution,
        SourceSnapshotPinner sourcePinning,
        MaterializationAuthorizer authorizer,
        LeaseService leases,
        FenceValidator fences,
        PublicationVerifier verifiers,
        MaterializationExtensionRegistry extensions,
        IdGenerator ids,
        Clock clock) {

    public KernelPorts {
        if (repository == null || catalog == null || planner == null || execution == null
                || sourcePinning == null || authorizer == null || leases == null || fences == null
                || verifiers == null || extensions == null || ids == null || clock == null) {
            throw new IllegalArgumentException("every materialization port must be supplied");
        }
    }
}
