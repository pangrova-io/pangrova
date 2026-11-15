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
package org.pangrova.planner.generation;

import org.pangrova.planner.constants.ArtifactBuildReport;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBuildDetail;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.common.api.annotation.Internal;

import java.util.HashSet;

/** Wraps one normalized relational plan in the generic Native tabular export sink. */
@Internal
public final class TabularExportPlanBuilder {

    public static final String PLAN_NODE_ID = "artifact-export-tabular";

    private TabularExportPlanBuilder() {}

    /** Returns a deterministic streaming export plan without changing its relational input. */
    public static GenerationPlan build(byte[] normalizedPlan) {
        Plan source = parse(normalizedPlan);
        if (!source.getUnknownFields().asMap().isEmpty()
                || source.getRelationsCount() != 1
                || !source.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException(
                    "tabular export requires one canonical Substrait root relation");
        }
        RelRoot root = source.getRelations(0).getRoot();
        if (!root.hasInput() || root.getNamesCount() == 0
                || root.getNamesList().stream().anyMatch(String::isBlank)
                || new HashSet<>(root.getNamesList()).size() != root.getNamesCount()) {
            throw new IllegalArgumentException(
                    "tabular export requires unique non-blank output field names");
        }
        ArtifactBuildDetail detail = ArtifactBuildDetail.newBuilder()
                .setPlanNodeId(PLAN_NODE_ID)
                .setReportSchemaVersion(ArtifactBuildReport.SCHEMA_VERSION)
                .addAllInputFields(root.getNamesList())
                .setInvocation(CapabilityInvocation.newBuilder()
                        .setCapability(BuiltInCapabilityCatalog.tabularExportPlanRef()))
                .build();
        RelationDetailEnvelope envelope = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setArtifactBuild(detail)
                .build();
        Rel sink = Rel.newBuilder()
                .setExtensionSingle(io.substrait.proto.ExtensionSingleRel.newBuilder()
                        .setInput(root.getInput())
                        .setDetail(Any.pack(envelope)))
                .build();
        Plan.Builder result = source.toBuilder()
                .clearRelations()
                .addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                        .setInput(sink)));
        SubstraitProtocolVersion.stamp(result);
        return new GenerationPlan(
                result.build().toByteArray(), PLAN_NODE_ID,
                BuiltInCapabilityCatalog.tabularExportPlanRef(),
                BuiltInCapabilityCatalog.tabularExportFormat(),
                BuiltInCapabilityCatalog.physicalRowLocatorContract());
    }

    private static Plan parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("normalizedPlan must not be empty");
        }
        try {
            return org.pangrova.protocol.substrait.SubstraitPlanCodec.parse(bytes);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("normalizedPlan is not Substrait", failure);
        }
    }
}
