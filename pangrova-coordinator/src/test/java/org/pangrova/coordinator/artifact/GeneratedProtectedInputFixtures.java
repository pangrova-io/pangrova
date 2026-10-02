package org.pangrova.coordinator.artifact;

import io.substrait.proto.Plan;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.Constraint;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Pure compiler-port contract fixture; SQL policy enforcement is verified by the actual E2E dispatcher. */
final class GeneratedProtectedInputFixtures {
    private GeneratedProtectedInputFixtures() {}

    static DeferredAuthorizationScope.PolicySnapshot observations() {
        return new DeferredAuthorizationScope.PolicySnapshot(Optional.empty(),
                Map.of("title", Optional.empty(), "body", Optional.empty(), "embedding", Optional.empty()));
    }

    static AuthorizedRelation compiled() {
        List<String> columns = List.of("title", "body", "embedding");
        var schema = org.pangrova.planner.substrait.NativeDatasetSchemaCompiler
                .compile(GeneratedBuildTestConnectors.sdkColumns()).schema();
        var read = io.substrait.proto.ReadRel.newBuilder().setBaseSchema(schema).setNamedTable(
                io.substrait.proto.ReadRel.NamedTable.newBuilder().addAllNames(List.of("lake", "sales", "documents")));
        var project = io.substrait.proto.ProjectRel.newBuilder().setInput(io.substrait.proto.Rel.newBuilder().setRead(read))
                .setCommon(io.substrait.proto.RelCommon.newBuilder().setEmit(
                        io.substrait.proto.RelCommon.Emit.newBuilder().addOutputMapping(0).addOutputMapping(1)));
        var plan = Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder().setRoot(
                io.substrait.proto.RelRoot.newBuilder().setInput(io.substrait.proto.Rel.newBuilder().setProject(project))
                        .addAllNames(List.of("title", "body"))));
        SubstraitProtocolVersion.stamp(plan);
        byte[] bytes = plan.build().toByteArray();
        var source = new SplitSourceDescriptor("lake", "sales", "documents", Constraint.EMPTY, columns);
        var graph = new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(new AuthorizedStageGraph.SerializedStage(0, 1, "OUTPUT",
                        new SerializedPlanFragment(bytes, List.of(), List.of()), List.of(source),
                        List.of("title", "body"), List.of(), 0, 0)), List.of());
        return new AuthorizedRelation(bytes, List.of(source), List.of("title", "body"),
                List.of("VARCHAR", "VARCHAR"), "0".repeat(64), true,
                Map.of(source.qualifiedName(), schema.toByteArray()),
                Map.of(source.qualifiedName(), observations()), graph);
    }
}
