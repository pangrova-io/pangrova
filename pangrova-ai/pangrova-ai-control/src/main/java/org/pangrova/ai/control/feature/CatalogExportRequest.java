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
package org.pangrova.ai.control.feature;

import org.pangrova.ai.control.enums.CatalogExportKind;

import java.time.Instant;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Immutable link between one catalog object version and its artifact build job. */
public record CatalogExportRequest(
        String tenantId,
        CatalogExportKind objectKind,
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

    public CatalogExportRequest {
        requireText(tenantId, "tenantId");
        objectKind = Objects.requireNonNull(objectKind, "objectKind must not be null");
        requireText(catalogName, "catalogName");
        requireText(schemaName, "schemaName");
        requireText(objectName, "objectName");
        if (objectVersion <= 0) {
            throw new IllegalArgumentException("objectVersion must be positive");
        }
        requireText(generation, "generation");
        requireText(artifactId, "artifactId");
        requireText(jobId, "jobId");
        requireSha(dependencySha256, "dependencySha256");
        requireText(requestedBy, "requestedBy");
        requestedAt = Objects.requireNonNull(requestedAt, "requestedAt must not be null");
    }

    private static void requireSha(String value, String name) {
        if (value == null || !value.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException(name + " must be a SHA-256 digest");
        }
    }
}
