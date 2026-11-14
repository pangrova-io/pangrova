package org.pangrova.materialization.usecase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.enums.MaterializationAction;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.MaterializationAuthorizer;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;

/** Authorizes the cascade before dropping logical objects in dependency order. */
final class MaterializationRemoval {
    private final MaterializationRepository repository;
    private final MaterializationAuthorizer authorizer;
    private final Clock clock;
    private final MaterializationSchedules schedules;

    MaterializationRemoval(MaterializationRepository repository, MaterializationAuthorizer authorizer,
            Clock clock, MaterializationSchedules schedules) {
        this.repository = repository;
        this.authorizer = authorizer;
        this.clock = clock;
        this.schedules = schedules;
    }

    /** Drops the logical name; physical generations remain until GC proves them unpinned. */
    void drop(DropMaterialization command) {
        MaterializationObject header = repository.findObject(command.object())
                .orElse(null);
        if (header == null) {
            if (command.ifExists()) {
                return;
            }
            throw MaterializationException.notFound(command.object().canonicalName());
        }
        if (header.state() == ObjectState.DROPPED) {
            return;
        }
        authorizer.requireManage(command.object(), command.requester(),
                MaterializationAction.DROP);
        List<MaterializationId> order = new ArrayList<>();
        collectDependentsFirst(command.object(), new java.util.LinkedHashSet<>(), order);
        if (order.size() > 1 && !command.cascade()) {
            order.remove(command.object());
            throw MaterializationException.dependencyConflict(
                    "materialization " + command.object().canonicalName() + " is referenced by "
                            + order.stream().map(MaterializationId::canonicalName)
                                    .collect(java.util.stream.Collectors.joining(", "))
                            + "; drop the dependents first or use CASCADE");
        }
        for (MaterializationId target : order) {
            if (!target.equals(command.object())) {
                authorizer.requireManage(target, command.requester(),
                        MaterializationAction.DROP);
            }
        }
        for (MaterializationId target : order) {
            dropOne(target, command);
        }
    }

    private void collectDependentsFirst(MaterializationId object,
            Set<MaterializationId> visited, List<MaterializationId> order) {
        if (!visited.add(object)) {
            return;
        }
        for (MaterializationId dependent : repository.findLiveDependents(object)) {
            collectDependentsFirst(dependent, visited, order);
        }
        order.add(object);
    }

    private void dropOne(MaterializationId object, DropMaterialization command) {
        MaterializationObject header = repository.findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object.canonicalName()));
        if (header.state() == ObjectState.DROPPED) {
            return;
        }
        schedules.suspendIfActive(object);
        Instant now = clock.now();
        MaterializationObject dropping = header;
        if (header.state() != ObjectState.DROPPING) {
            dropping = header.transitionTo(ObjectState.DROPPING, header.revision(), now);
            repository.updateObject(dropping, header.revision(), command.commandId(),
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, object,
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "dropping", now));
        }
        MaterializationObject dropped = dropping.dropped(dropping.revision(), now);
        repository.dropObject(dropped, dropping.publishedGeneration(),
                dropping.revision(), command.commandId(),
                MaterializationEvents.of(MaterializationEventType.OBJECT_DROPPED, object,
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(), "dropped", now));
    }
}
