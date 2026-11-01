/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/** Tenant-scoped immutable Feature View and Training Dataset catalog. */
public interface FeatureCatalogStore {

    void createFeatureView(FeatureViewVersion version);

    Optional<FeatureViewVersion> findFeatureView(FeatureViewId id, long version);

    Optional<FeaturePublication> publishFeatureView(
            FeatureViewId id, long version, OptionalLong expectedPublicationEpoch,
            String publishedBy, Instant publishedAt);

    void createTrainingDataset(TrainingDatasetVersion version);

    Optional<TrainingDatasetVersion> findTrainingDataset(
            TrainingDatasetId id, long version);

    Optional<ExportRequest> findExport(
            String tenantId, ObjectKind kind, String catalogName,
            String schemaName, String objectName, long version, String generation);

    enum ObjectKind { FEATURE_VIEW, TRAINING_DATASET }

    enum SplitKind { RANDOM, TEMPORAL, ENTITY }

    record FeatureViewId(
            String tenantId, String catalogName, String schemaName, String viewName) {
        public FeatureViewId {
            requireText(tenantId, "tenantId");
            requireText(catalogName, "catalogName");
            requireText(schemaName, "schemaName");
            requireText(viewName, "viewName");
        }
    }

    record TrainingDatasetId(
            String tenantId, String catalogName, String schemaName, String datasetName) {
        public TrainingDatasetId {
            requireText(tenantId, "tenantId");
            requireText(catalogName, "catalogName");
            requireText(schemaName, "schemaName");
            requireText(datasetName, "datasetName");
        }
    }

    record FeatureViewVersion(
            FeatureViewId id,
            long version,
            String ownerPrincipal,
            List<String> entityKeys,
            String eventTimeColumn,
            String sourceTable,
            String sourceQuery,
            byte[] sourcePlan,
            String sourcePlanSha256,
            String transformVersion,
            String schemaJson,
            long freshnessTargetMillis,
            String lineageJson,
            String definitionJson,
            String definitionSha256,
            Instant createdAt) {
        public FeatureViewVersion {
            id = Objects.requireNonNull(id, "id must not be null");
            requirePositive(version, "version");
            requireText(ownerPrincipal, "ownerPrincipal");
            entityKeys = List.copyOf(entityKeys);
            if (entityKeys.isEmpty() || entityKeys.stream().anyMatch(FeatureCatalogStore::blank)) {
                throw new IllegalArgumentException("entityKeys must not be empty or blank");
            }
            if (Set.copyOf(entityKeys).size() != entityKeys.size()) {
                throw new IllegalArgumentException("entityKeys must be unique");
            }
            requireText(eventTimeColumn, "eventTimeColumn");
            requireText(sourceTable, "sourceTable");
            requireText(sourceQuery, "sourceQuery");
            sourcePlan = requireBytes(sourcePlan, "sourcePlan");
            requireSha(sourcePlanSha256, "sourcePlanSha256");
            requireText(transformVersion, "transformVersion");
            requireText(schemaJson, "schemaJson");
            requirePositive(freshnessTargetMillis, "freshnessTargetMillis");
            requireText(lineageJson, "lineageJson");
            requireText(definitionJson, "definitionJson");
            requireSha(definitionSha256, "definitionSha256");
            createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        }

        @Override public byte[] sourcePlan() { return sourcePlan.clone(); }
    }

    record FeaturePublication(
            FeatureViewId id, long activeVersion, long publicationEpoch,
            String publishedBy, Instant publishedAt) {
        public FeaturePublication {
            id = Objects.requireNonNull(id, "id must not be null");
            requirePositive(activeVersion, "activeVersion");
            requirePositive(publicationEpoch, "publicationEpoch");
            requireText(publishedBy, "publishedBy");
            publishedAt = Objects.requireNonNull(publishedAt, "publishedAt must not be null");
        }
    }

    record TrainingDatasetVersion(
            TrainingDatasetId id,
            long version,
            String ownerPrincipal,
            Map<String, String> sourceSnapshots,
            Map<String, Long> featureVersions,
            String labelColumn,
            byte[] normalizedPlan,
            String normalizedPlanSha256,
            SplitKind splitKind,
            long splitSeed,
            String splitSpecJson,
            String schemaJson,
            String lineageJson,
            String definitionJson,
            String definitionSha256,
            Instant createdAt) {
        public TrainingDatasetVersion {
            id = Objects.requireNonNull(id, "id must not be null");
            requirePositive(version, "version");
            requireText(ownerPrincipal, "ownerPrincipal");
            sourceSnapshots = Map.copyOf(sourceSnapshots);
            featureVersions = Map.copyOf(featureVersions);
            if (sourceSnapshots.isEmpty()
                    || sourceSnapshots.entrySet().stream().anyMatch(entry ->
                    blank(entry.getKey()) || blank(entry.getValue()))) {
                throw new IllegalArgumentException("sourceSnapshots must pin every source");
            }
            if (featureVersions.entrySet().stream().anyMatch(entry ->
                    blank(entry.getKey()) || entry.getValue() == null || entry.getValue() <= 0)) {
                throw new IllegalArgumentException("featureVersions must contain positive versions");
            }
            requireText(labelColumn, "labelColumn");
            normalizedPlan = requireBytes(normalizedPlan, "normalizedPlan");
            requireSha(normalizedPlanSha256, "normalizedPlanSha256");
            splitKind = Objects.requireNonNull(splitKind, "splitKind must not be null");
            requireText(splitSpecJson, "splitSpecJson");
            requireText(schemaJson, "schemaJson");
            requireText(lineageJson, "lineageJson");
            requireText(definitionJson, "definitionJson");
            requireSha(definitionSha256, "definitionSha256");
            createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        }

        @Override public byte[] normalizedPlan() { return normalizedPlan.clone(); }
    }

    record ExportRequest(
            String tenantId,
            ObjectKind objectKind,
            String catalogName,
            String schemaName,
            String objectName,
            long objectVersion,
            String generation,
            String artifactId,
            String jobId,
            String dependencySha256,
            String requestedBy,
            Instant requestedAt) {
        public ExportRequest {
            requireText(tenantId, "tenantId");
            objectKind = Objects.requireNonNull(objectKind, "objectKind must not be null");
            requireText(catalogName, "catalogName");
            requireText(schemaName, "schemaName");
            requireText(objectName, "objectName");
            requirePositive(objectVersion, "objectVersion");
            requireText(generation, "generation");
            requireText(artifactId, "artifactId");
            requireText(jobId, "jobId");
            requireSha(dependencySha256, "dependencySha256");
            requireText(requestedBy, "requestedBy");
            requestedAt = Objects.requireNonNull(requestedAt, "requestedAt must not be null");
        }
    }

    private static byte[] requireBytes(byte[] value, String name) {
        if (value == null || value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.clone();
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireText(String value, String name) {
        if (blank(value)) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireSha(String value, String name) {
        if (value == null || !value.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException(name + " must be a SHA-256 digest");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
