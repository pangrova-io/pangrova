package org.pangrova.materialization.kernel.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.DEFINER;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.INCARNATION;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.LATER;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.kernel.MaterializationTestFixtures.objectId;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.StrategyId;

class MaterializationObjectTest {

    private static final MaterializationId OBJECT = objectId("orders_dt");
    private static final GenerationId GENERATION = new GenerationId(
            java.util.UUID.fromString("00000000-0000-0000-0000-0000000000aa"));

    @Test
    void createsHeaderWithNoPublishedGeneration() {
        MaterializationObject object = creating();

        assertThat(object.revision()).isZero();
        assertThat(object.definitionRevision()).isEqualTo(1);
        assertThat(object.publishedGeneration()).isEmpty();
        assertThat(object.state()).isEqualTo(ObjectState.Creating.INSTANCE);
        assertThat(object.incarnation()).isEqualTo(INCARNATION);
        assertThat(object.definer()).isEqualTo(DEFINER);
    }

    @Test
    void publishAdvancesRevisionAndActivatesTheObject() {
        MaterializationObject creating = creating();
        MaterializationObject published = creating.publish(GENERATION, 0, LATER);

        assertThat(published.revision()).isEqualTo(1);
        assertThat(published.publishedGeneration()).contains(GENERATION);
        assertThat(published.state()).isEqualTo(ObjectState.Active.INSTANCE);
        assertThat(published.definitionRevision()).isEqualTo(creating.definitionRevision());
        assertThat(published.incarnation()).isEqualTo(creating.incarnation());
        assertThat(published.updatedAt()).isEqualTo(LATER);
    }

    @Test
    void staleHeadRevisionRejectsEveryMutation() {
        MaterializationObject published = creating().publish(GENERATION, 0, LATER);

        MaterializationException publishConflict = catchThrowableOfType(
                () -> published.publish(GENERATION, 0, LATER), MaterializationException.class);
        MaterializationException transitionConflict = catchThrowableOfType(
                () -> published.transitionTo(ObjectState.Suspended.INSTANCE, 0, LATER),
                MaterializationException.class);
        MaterializationException definitionConflict = catchThrowableOfType(
                () -> published.withDefinitionRevision(2, 0, LATER), MaterializationException.class);

        assertThat(publishConflict.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION);
        assertThat(transitionConflict.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION);
        assertThat(definitionConflict.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION);
    }

    @Test
    void lifecycleTransitionsFollowTheStateMachine() {
        MaterializationObject active = creating().publish(GENERATION, 0, LATER);
        MaterializationObject suspended = active.transitionTo(ObjectState.Suspended.INSTANCE, 1, LATER);
        MaterializationObject resumed = suspended.transitionTo(ObjectState.Active.INSTANCE, 2, LATER);
        MaterializationObject dropping = resumed.transitionTo(ObjectState.Dropping.INSTANCE, 3, LATER);
        MaterializationObject dropped = dropping.transitionTo(ObjectState.Dropped.INSTANCE, 4, LATER);

        assertThat(suspended.state()).isEqualTo(ObjectState.Suspended.INSTANCE);
        assertThat(suspended.publishedGeneration()).contains(GENERATION);
        assertThat(resumed.state()).isEqualTo(ObjectState.Active.INSTANCE);
        assertThat(dropped.state()).isEqualTo(ObjectState.Dropped.INSTANCE);
        assertThat(dropped.revision()).isEqualTo(5);

        MaterializationException illegal = catchThrowableOfType(
                () -> dropped.transitionTo(ObjectState.Active.INSTANCE, 5, LATER),
                MaterializationException.class);
        assertThat(illegal.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void publishIsIllegalWhileDropping() {
        MaterializationObject dropping = creating()
                .transitionTo(ObjectState.Dropping.INSTANCE, 0, LATER);

        MaterializationException exception = catchThrowableOfType(
                () -> dropping.publish(GENERATION, 1, LATER), MaterializationException.class);

        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void definitionRevisionMustAdvanceMonotonically() {
        MaterializationObject object = creating();
        MaterializationObject altered = object.withDefinitionRevision(2, 0, LATER);

        assertThat(altered.definitionRevision()).isEqualTo(2);
        assertThat(altered.revision()).isEqualTo(1);
        assertThat(altered.publishedGeneration()).isEmpty();
        assertThatIllegalArgumentException()
                .isThrownBy(() -> altered.withDefinitionRevision(2, 1, LATER))
                .withMessageContaining("must advance");
    }

    @Test
    void fullRefreshModeCannotCarryAnIncrementalStrategy() {
        assertThatIllegalArgumentException().isThrownBy(() -> MaterializationObject.creating(
                        OBJECT,
                        MaterializationKind.DYNAMIC_TABLE,
                        INCARNATION,
                        DEFINER,
                        SecurityMode.DEFINER,
                        RefreshMode.FULL,
                        Optional.of(StrategyId.APPEND_ONLY),
                        NOW))
                .withMessageContaining("FULL refresh mode cannot carry an incremental strategy");

        MaterializationObject object = creating();
        assertThatIllegalArgumentException()
                .isThrownBy(() -> object.withRefreshConfiguration(
                        RefreshMode.FULL, Optional.of(StrategyId.APPEND_ONLY), 0, LATER))
                .withMessageContaining("FULL refresh mode cannot carry an incremental strategy");
    }

    @Test
    void refreshConfigurationChangeAdvancesRevision() {
        MaterializationObject object = creating();
        MaterializationObject incremental = object.withRefreshConfiguration(
                RefreshMode.INCREMENTAL, Optional.of(StrategyId.APPEND_ONLY), 0, LATER);

        assertThat(incremental.refreshMode()).isEqualTo(RefreshMode.INCREMENTAL);
        assertThat(incremental.incrementalStrategy()).contains(StrategyId.APPEND_ONLY);
        assertThat(incremental.revision()).isEqualTo(1);
    }

    @Test
    void creatingHeaderCannotCarryAPublishedGeneration() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MaterializationObject(
                        OBJECT,
                        MaterializationKind.DYNAMIC_TABLE,
                        INCARNATION,
                        1,
                        0,
                        Optional.of(GENERATION),
                        ObjectState.Creating.INSTANCE,
                        DEFINER,
                        SecurityMode.DEFINER,
                        RefreshMode.FULL,
                        Optional.empty(),
                        NOW,
                        NOW))
                .withMessageContaining("creating object cannot have a published generation");
    }

    @Test
    void timestampsAndIdentitiesAreMandatory() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MaterializationObject(
                        OBJECT,
                        MaterializationKind.DYNAMIC_TABLE,
                        INCARNATION,
                        1,
                        0,
                        Optional.empty(),
                        ObjectState.Creating.INSTANCE,
                        DEFINER,
                        SecurityMode.DEFINER,
                        RefreshMode.FULL,
                        Optional.empty(),
                        LATER,
                        NOW))
                .withMessageContaining("updatedAt must not precede createdAt");
    }

    private static MaterializationObject creating() {
        return MaterializationObject.creating(
                OBJECT,
                MaterializationKind.DYNAMIC_TABLE,
                INCARNATION,
                DEFINER,
                SecurityMode.DEFINER,
                RefreshMode.FULL,
                Optional.empty(),
                NOW);
    }
}
