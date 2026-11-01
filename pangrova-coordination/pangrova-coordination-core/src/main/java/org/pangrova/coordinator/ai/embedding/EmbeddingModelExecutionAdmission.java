/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.scheduler.core.placement.WorkerCapabilityMatcher;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.ai.embedding.EmbeddingModelBindingIdentity;

import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Enforces the catalog fence for one exact Worker embedding deployment. */
public final class EmbeddingModelExecutionAdmission {

    private EmbeddingModelExecutionAdmission() {}

    /** Binds a new catalog descriptor to an active exact Worker deployment. */
    public static EmbeddingModelDescriptor bind(
            EmbeddingModelDescriptor descriptor, ModelAvailability availability) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        requireAdvertisedContract(descriptor, availability, false);
        return new EmbeddingModelDescriptor(
                descriptor.name(), descriptor.route(), descriptor.contract(),
                descriptor.limits(), descriptor.version(),
                new EmbeddingModelDescriptor.DeploymentFence(
                        availability.getModel().getVersion(), signature(availability)));
    }

    /** Requires an active Worker advertisement to match the persisted catalog fence. */
    public static void requireCompatible(
            EmbeddingModelDescriptor descriptor, ModelAvailability availability) {
        requireAdvertisedContract(descriptor, availability, true);
        if (!descriptor.deploymentVersion().equals(availability.getModel().getVersion())
                || !descriptor.deploymentSignatureSha256().equals(signature(availability))) {
            throw validation("Worker deployment identity does not match catalog revision for model '"
                    + descriptor.name() + "'");
        }
    }

    /** Returns whether this is the canonical embedding capability. */
    public static boolean isEmbeddingCapability(CapabilityRef capability) {
        return WorkerCapabilityMatcher.matchesCapabilityRef(
                BuiltInCapabilityCatalog.aiEmbeddingPlanRef(), capability);
    }

    private static void requireAdvertisedContract(
            EmbeddingModelDescriptor descriptor,
            ModelAvailability availability,
            boolean requireBound) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        Objects.requireNonNull(availability, "availability must not be null");
        if (requireBound && !descriptor.isDeploymentBound()) {
            throw validation("Embedding model catalog revision is not bound to an active deployment: "
                    + descriptor.name());
        }
        if (!descriptor.name().equals(availability.getModel().getId())) {
            throw validation("Worker model name does not match catalog model '"
                    + descriptor.name() + "'");
        }
        ModelCapabilityContract contract = embeddingContract(availability, descriptor.name());
        byte[] advertisedBinding = contract.getBindingContractSha256().toByteArray();
        if (advertisedBinding.length != 32
                || !java.security.MessageDigest.isEqual(
                        advertisedBinding,
                        EmbeddingModelBindingIdentity.sha256(descriptor))) {
            throw validation("Worker embedding execution configuration does not match catalog "
                    + "contract for model '" + descriptor.name() + "'");
        }
        int dimension = VectorLogicalType.requireDimension(contract.getOutputType());
        if (dimension != descriptor.dimension()) {
            throw validation("Worker embedding dimension " + dimension
                    + " does not match catalog dimension " + descriptor.dimension()
                    + " for model '" + descriptor.name() + "'");
        }
        if (availability.getLimits().getMaxBatchItems() != descriptor.maxBatchSize()) {
            throw validation("Worker max batch size does not match catalog contract for model '"
                    + descriptor.name() + "'");
        }
    }

    private static ModelCapabilityContract embeddingContract(
            ModelAvailability availability, String modelName) {
        CapabilityRef required = BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
        List<ModelCapabilityContract> matches = availability.getCapabilityContractsList().stream()
                .filter(contract -> WorkerCapabilityMatcher.matchesCapabilityRef(
                        contract.getCapability(), required))
                .toList();
        if (matches.size() != 1) {
            throw validation("Worker must advertise one exact embedding output for model '"
                    + modelName + "'");
        }
        return matches.get(0);
    }

    private static String signature(ModelAvailability availability) {
        byte[] signature = availability.getModel().getSignatureSha256().toByteArray();
        if (signature.length != 32) {
            throw validation("Worker model has an invalid deployment signature: "
                    + availability.getModel().getId());
        }
        return HexFormat.of().formatHex(signature);
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
