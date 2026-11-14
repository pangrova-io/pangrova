package org.pangrova.operations.dr;

import org.pangrova.common.annotation.Experimental;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates failover and failback between primary and standby clusters.
 *
 * <p>This controller has real orchestration logic (step sequencing, error handling)
 * but no production {@link StandbyOperations} implementation exists. The failover
 * pipeline is untested against real infrastructure.</p>
 */
@Experimental(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-failover")
public final class FailoverController {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverController.class);

    /** Executes the full failover sequence promoting the standby cluster. */
    public void failover(StandbyOperations ops) {
        LOG.info("event=failover_started");
        executeStep(FailoverStep.VERIFY_STANDBY, ops::verifyHealth);
        executeStep(FailoverStep.CHECK_LAG, ops::checkLag);
        executeStep(FailoverStep.STOP_WRITES, ops::stopWrites);
        executeStep(FailoverStep.WAIT_CATCHUP, ops::waitCatchup);
        executeStep(FailoverStep.PROMOTE, ops::promote);
        executeStep(FailoverStep.UPDATE_DNS, ops::updateDns);
        executeStep(FailoverStep.NOTIFY, ops::notifyClients);
        LOG.info("event=failover_completed");
    }

    /**
     * Executes failback sequence. CHECK_LAG is intentionally skipped because
     * during failback the original primary has been receiving replicated writes
     * and WAIT_CATCHUP ensures it is fully caught up before promotion.
     */
    public void failback(StandbyOperations ops) {
        LOG.info("event=failback_started");
        executeStep(FailoverStep.VERIFY_STANDBY, ops::verifyHealth);
        executeStep(FailoverStep.STOP_WRITES, ops::stopWrites);
        executeStep(FailoverStep.WAIT_CATCHUP, ops::waitCatchup);
        executeStep(FailoverStep.PROMOTE, ops::promote);
        executeStep(FailoverStep.UPDATE_DNS, ops::updateDns);
        executeStep(FailoverStep.NOTIFY, ops::notifyClients);
        LOG.info("event=failback_completed");
    }

    private void executeStep(FailoverStep step, Runnable action) {
        LOG.info("event=failover_step_executing step={}", step);
        try {
            action.run();
        } catch (Exception e) {
            throw new PangrovaException(PangrovaErrorCode.OPS_FAILOVER_VERIFICATION_FAILED,
                    "Failover step " + step + " failed: " + e.getMessage(), e);
        }
    }

    public enum FailoverStep {
        VERIFY_STANDBY, CHECK_LAG, STOP_WRITES, WAIT_CATCHUP, PROMOTE, UPDATE_DNS, NOTIFY
    }

    /**
     * SPI for standby cluster operations during failover.
     */
    public interface StandbyOperations {

        void verifyHealth();

        void checkLag();

        void stopWrites();

        void waitCatchup();

        void promote();

        void updateDns();

        void notifyClients();
    }
}
