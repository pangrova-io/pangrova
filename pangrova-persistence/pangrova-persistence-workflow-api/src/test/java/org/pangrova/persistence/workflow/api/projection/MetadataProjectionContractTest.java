package org.pangrova.persistence.workflow.api.projection;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class MetadataProjectionContractTest {
    @Test void callerCannotChangeOperationContentAfterIdentityHasBeenAssigned() {
        var scope = new CatalogDeploymentIdentity("cat", "incarnation", 1);
        var names = new ArrayList<>(List.of("z", "a", "z"));
        UUID id = UUID.randomUUID();
        var mutation = new MetadataProjectionMutation.ObserveSchemas(id, scope, names);
        names.clear();
        assertThat(mutation.schemas()).containsExactly("a", "z");
        assertThatThrownBy(() -> mutation.schemas().add("foreign")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(mutation).isEqualTo(new MetadataProjectionMutation.ObserveSchemas(id, scope, List.of("a", "z")));
    }

    @Test void receiptTenantImpactIsAnImmutableHistoricalSnapshot() {
        var mutation = new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(),
                new CatalogDeploymentIdentity("cat", "incarnation", 1), List.of());
        var tenants = new HashSet<>(Set.of("tenant-a"));
        var receipt = new MetadataProjectionReceipt(mutation, false, Optional.empty(), tenants);
        tenants.clear();
        assertThat(receipt.allowedTenants()).containsExactly("tenant-a");
        assertThatThrownBy(() -> receipt.allowedTenants().add("tenant-b")).isInstanceOf(UnsupportedOperationException.class);
    }
}
