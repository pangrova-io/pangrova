package org.pangrova.worker.copy;

import java.util.Objects;
import java.util.function.Consumer;

/** Exact task allowance and cleanup owner, installed before the managed read starts. */
public record CopyMaterialization(CopyTaskReadBudget budget, Consumer<VerifiedLocalCopySource> publishOwner) {
    public CopyMaterialization {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(publishOwner, "publishOwner");
    }

    /** Preparation outside an executing task; Native admission supplies its actual owner instead. */
    static CopyMaterialization standalone() {
        return new CopyMaterialization(new CopyTaskReadBudget(Long.MAX_VALUE), ignored -> {});
    }
}
