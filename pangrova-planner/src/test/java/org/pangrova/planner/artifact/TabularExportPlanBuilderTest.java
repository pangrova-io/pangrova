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
package org.pangrova.planner.artifact;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TabularExportPlanBuilderTest {

    @Test
    void wrapsOneCanonicalRelationInGenericNativeExportSink() throws Exception {
        ArtifactBuildPlan export = TabularExportPlanBuilder.build(sourcePlan());

        assertThat(export.capability())
                .isEqualTo(BuiltInCapabilityCatalog.tabularExportPlanRef());
        assertThat(export.format())
                .isEqualTo(BuiltInCapabilityCatalog.tabularExportFormat());
        Plan plan = Plan.parseFrom(export.planBytes());
        SubstraitProtocolVersion.validate(plan);
        var sink = plan.getRelations(0).getRoot().getInput().getExtensionSingle();
        assertThat(plan.getRelations(0).getRoot().getNamesCount()).isZero();
        assertThat(SubstraitRelationLayout.rootSchema(plan.getRelations(0).getRoot())
                .getStruct().getTypesCount()).isZero();
        RelationDetailEnvelope detail = sink.getDetail().unpack(
                RelationDetailEnvelope.class);
        assertThat(detail.getArtifactBuild().getInputFieldsList())
                .containsExactly("customer_id", "event_ts");
        assertThat(detail.getArtifactBuild().getInvocation().getArgumentsCount()).isZero();
        assertThat(sink.getInput().hasRead()).isTrue();
    }

    @Test
    void rejectsAmbiguousOutputNames() throws Exception {
        Plan source = Plan.parseFrom(sourcePlan());
        byte[] ambiguous = source.toBuilder().setRelations(0,
                source.getRelations(0).toBuilder().setRoot(
                        source.getRelations(0).getRoot().toBuilder()
                                .setNames(1, "customer_id"))).build().toByteArray();

        assertThatThrownBy(() -> TabularExportPlanBuilder.build(ambiguous))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    private static byte[] sourcePlan() {
        NamedStruct schema = NamedStruct.newBuilder()
                .addNames("customer_id")
                .addNames("event_ts")
                .setStruct(Type.Struct.newBuilder()
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED))))
                .build();
        Rel read = Rel.newBuilder().setRead(ReadRel.newBuilder()
                .setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder()
                        .addNames("lake").addNames("raw").addNames("customers")))
                .build();
        Plan.Builder plan = Plan.newBuilder().addRelations(PlanRel.newBuilder()
                .setRoot(RelRoot.newBuilder().setInput(read)
                        .addNames("customer_id").addNames("event_ts")));
        SubstraitProtocolVersion.stamp(plan);
        return plan.build().toByteArray();
    }
}
