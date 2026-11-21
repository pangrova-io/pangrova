package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;

/** Applies schedule changes under the durable schedule version. */
final class MaterializationSchedules {
    private final MaterializationRepository repository;

    MaterializationSchedules(MaterializationRepository repository) {
        this.repository = repository;
    }

    Schedule update(MaterializationId object,
            java.util.function.UnaryOperator<Schedule> mutation) {
        for (int attempt = 0; attempt < 3; attempt++) {
            Schedule current = repository.findSchedule(object)
                    .orElseThrow(() -> MaterializationException.requestRejected(
                            "materialization " + object.canonicalName() + " has no schedule"));
            Schedule updated = mutation.apply(current);
            if (updated.version() == current.version()) {
                return current;
            }
            try {
                repository.updateSchedule(updated, current.version());
                return updated;
            } catch (MaterializationException conflict) {
                if (conflict.getErrorCode()
                        != org.pangrova.common.enums.PangrovaErrorCode
                                .MATERIALIZATION_CONCURRENT_PUBLICATION) {
                    throw conflict;
                }
            }
        }
        throw MaterializationException.concurrentPublication(
                "could not update the schedule of " + object.canonicalName());
    }

    void suspendIfActive(MaterializationId object) {
        if (repository.findSchedule(object)
                .filter(schedule -> schedule.state()
                        == org.pangrova.materialization.api.enums.ScheduleState.ACTIVE)
                .isEmpty()) {
            return;
        }
        update(object, schedule -> schedule.state()
                == org.pangrova.materialization.api.enums.ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }
}
