package org.pangrova.materialization.usecase;

import org.pangrova.materialization.extension.MaterializationExtensionRegistry;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.FenceValidator;
import org.pangrova.materialization.port.IdGenerator;
import org.pangrova.materialization.port.LeaseService;
import org.pangrova.materialization.port.MaterializationAuthorizer;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.PublicationVerifier;
import org.pangrova.materialization.port.SourcePinning;

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
        SourcePinning sourcePinning,
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
