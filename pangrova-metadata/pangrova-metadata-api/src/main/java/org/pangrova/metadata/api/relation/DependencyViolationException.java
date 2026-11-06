package org.pangrova.metadata.api.relation;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetadataException;

import java.util.List;
import java.util.Objects;

/**
 * Thrown when an attempted DROP would orphan one or more dependent objects
 * and the active {@link CascadePolicy} is {@link CascadePolicy#RESTRICT}.
 *
 * <p>The error code is reused: {@link PangrovaErrorCode#METADATA_SCHEMA_INCOMPATIBLE}
 * — semantically "the requested schema operation is incompatible with current
 * dependency state." A dedicated code may be added later if external clients
 * need to discriminate.
 */
public class DependencyViolationException extends MetadataException {

    private final ObjectRef parent;
    private final List<ObjectRef> dependents;

    public DependencyViolationException(ObjectRef parent, List<ObjectRef> dependents) {
        super(PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE, formatMessage(parent, dependents));
        this.parent = Objects.requireNonNull(parent, "parent");
        this.dependents = List.copyOf(Objects.requireNonNull(dependents, "dependents"));
    }

    public ObjectRef parent() {
        return parent;
    }

    public List<ObjectRef> dependents() {
        return dependents;
    }

    private static String formatMessage(ObjectRef parent, List<ObjectRef> dependents) {
        return "Cannot drop " + parent + " — " + dependents.size()
                + " dependent object(s) exist: " + dependents
                + ". Use CASCADE to drop dependents, or remove them first.";
    }
}
