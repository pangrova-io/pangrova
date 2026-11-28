package org.pangrova.persistence.materialization.api.publication;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.PublishedMaterialization;

/** Immutable publication directory and its tenant revision, observed in one database snapshot. */
@Experimental
public final class MaterializationPublicationSnapshot {
    private final TenantId tenant;
    private final long revision;
    private final List<PublishedMaterialization> publications;
    private final Map<QualifiedName, PublishedMaterialization> byName;

    /** Indexes the authoritative tenant publications in catalog order. */
    public MaterializationPublicationSnapshot(TenantId tenant, long revision,
            List<PublishedMaterialization> publications) {
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        if (revision < 0 || revision == 0 && !publications.isEmpty()) {
            throw new IllegalArgumentException("Invalid materialization publication revision: " + revision);
        }
        this.revision = revision;
        this.publications = List.copyOf(publications);
        var index = new HashMap<QualifiedName, PublishedMaterialization>();
        for (var publication : this.publications) {
            if (!tenant.equals(publication.object().tenant())
                    || index.putIfAbsent(publication.object().name(), publication) != null) {
                throw new IllegalArgumentException("Invalid publication identity: " + publication.object());
            }
        }
        this.byName = Map.copyOf(index);
    }

    public TenantId tenant() {
        return tenant;
    }

    public long revision() {
        return revision;
    }

    public List<PublishedMaterialization> publications() {
        return publications;
    }

    public Optional<PublishedMaterialization> find(QualifiedName name) {
        return Optional.ofNullable(byName.get(name));
    }
}
