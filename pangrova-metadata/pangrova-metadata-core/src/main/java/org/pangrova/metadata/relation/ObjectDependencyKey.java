package org.pangrova.metadata.relation;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.metadata.api.relation.ObjectRef;

/** Natural graph identity shared by stores and traversal; kind remains per-edge metadata. */
@Internal
public record ObjectDependencyKey(String catalog, String schema, String name) {
    public static ObjectDependencyKey of(ObjectRef ref) {
        return new ObjectDependencyKey(ref.catalog(), ref.schema(), ref.name());
    }
}
