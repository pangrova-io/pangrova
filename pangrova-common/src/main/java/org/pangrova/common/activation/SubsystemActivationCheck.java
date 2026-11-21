package org.pangrova.common.activation;

import org.pangrova.common.exception.SubsystemActivationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup self-check (SI-8, Requirement T14) that asserts every "wired-but-inert" subsystem from the
 * architecture-remediation activation inventory has been <em>activated</em> — not merely loaded.
 *
 * <p>The authoritative list of subsystem IDs ships as a classpath resource
 * ({@code org/pangrova/common/activation/activation-inventory.txt}, 170 items) generated from
 * {@code .kiro/specs/architecture-remediation/activation-inventory.md}. Each remediation task that
 * activates a subsystem registers an {@link #register(String, String, BooleanSupplier) activation
 * predicate} for its ID via {@link #register}. At bootstrap completion, {@link #runOrAbort} evaluates
 * every registered predicate; if any returns {@code false} (or throws), startup aborts listing the
 * offending IDs (design principle 4: "Fail fast and visibly").
 *
 * <p><b>Shim semantics (Phase P-0):</b> an inventory ID with no registered predicate is treated as a
 * not-yet-implemented shim and counted as passing, so the check is green before any remediation task
 * lands and turns progressively stricter as predicates are added. Registering a predicate for an ID
 * that is not in the inventory is rejected, to keep the inventory authoritative.
 *
 * <p>Instances are not thread-safe; register predicates during single-threaded bootstrap, then call
 * {@link #runOrAbort} once at the end.
 */
public final class SubsystemActivationCheck {

    private static final Logger LOG = LoggerFactory.getLogger(SubsystemActivationCheck.class);

    static final String INVENTORY_RESOURCE = "org/pangrova/common/activation/activation-inventory.txt";

    private final Set<String> inventoryIds;
    private final Map<String, Predicate> predicates = new LinkedHashMap<>();

    private SubsystemActivationCheck(Set<String> inventoryIds) {
        this.inventoryIds = inventoryIds;
    }

    /** Creates a check seeded from the bundled activation inventory resource. */
    public static SubsystemActivationCheck fromInventory() {
        return new SubsystemActivationCheck(loadInventory(INVENTORY_RESOURCE));
    }

    /** Visible for testing: create a check over an explicit set of IDs. */
    static SubsystemActivationCheck forIds(Set<String> ids) {
        return new SubsystemActivationCheck(new LinkedHashSet<>(ids));
    }

    /**
     * Registers an activation predicate for an inventory subsystem ID. The predicate returns
     * {@code true} when the subsystem is genuinely active (e.g. impl is not a NoOp/empty/test-ctor).
     *
     * @throws IllegalArgumentException if {@code id} is not in the activation inventory, or already
     *                                  has a registered predicate
     */
    public SubsystemActivationCheck register(String id, String description, BooleanSupplier predicate) {
        if (id == null || !inventoryIds.contains(id)) {
            throw new IllegalArgumentException(
                    "Unknown activation inventory ID: " + id + " (not present in " + INVENTORY_RESOURCE + ")");
        }
        if (predicates.containsKey(id)) {
            throw new IllegalArgumentException("Activation predicate already registered for ID: " + id);
        }
        if (predicate == null) {
            throw new IllegalArgumentException("predicate must not be null for ID: " + id);
        }
        predicates.put(id, new Predicate(description == null ? "" : description, predicate));
        return this;
    }

    /** Evaluates every registered predicate without aborting; returns a structured result. */
    public Result verify() {
        List<String> failed = new ArrayList<>();
        for (Map.Entry<String, Predicate> e : predicates.entrySet()) {
            boolean ok;
            try {
                ok = e.getValue().predicate.getAsBoolean();
            } catch (RuntimeException ex) {
                LOG.warn("event=activation_predicate_error id={} error={}", e.getKey(), ex.toString());
                ok = false;
            }
            if (!ok) {
                failed.add(e.getKey());
            }
        }
        int shimmed = inventoryIds.size() - predicates.size();
        return new Result(inventoryIds.size(), predicates.size(), shimmed, failed);
    }

    /**
     * Runs the self-check at bootstrap completion. Aborts startup (throws
     * {@link SubsystemActivationException}) if any registered predicate fails.
     */
    public void runOrAbort() {
        Result result = verify();
        if (result.allActive()) {
            LOG.info("event=activation_self_check_passed inventory={} active={} shimmed={}",
                    result.inventorySize(), result.activeCount(), result.shimmedCount());
            return;
        }
        LOG.error("event=activation_self_check_failed inventory={} active={} shimmed={} failed={}",
                result.inventorySize(), result.activeCount(), result.shimmedCount(), result.failedIds());
        throw new SubsystemActivationException(result.failedIds());
    }

    /** Number of subsystem IDs in the bundled inventory. */
    public int inventorySize() {
        return inventoryIds.size();
    }

    private static Set<String> loadInventory(String resource) {
        ClassLoader cl = SubsystemActivationCheck.class.getClassLoader();
        try (InputStream in = cl.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Activation inventory resource not found on classpath: " + resource);
            }
            Set<String> ids = new LinkedHashSet<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    ids.add(trimmed);
                }
            }
            return ids;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading activation inventory: " + resource, e);
        }
    }

    private static final class Predicate {
        final String description;
        final BooleanSupplier predicate;

        Predicate(String description, BooleanSupplier predicate) {
            this.description = description;
            this.predicate = predicate;
        }
    }

    /** Outcome of {@link #verify()}: counts plus the IDs whose predicates failed. */
    public static final class Result {
        private final int inventorySize;
        private final int activeCount;
        private final int shimmedCount;
        private final List<String> failedIds;

        Result(int inventorySize, int activeCount, int shimmedCount, List<String> failedIds) {
            this.inventorySize = inventorySize;
            this.activeCount = activeCount;
            this.shimmedCount = shimmedCount;
            this.failedIds = List.copyOf(failedIds);
        }

        public boolean allActive() {
            return failedIds.isEmpty();
        }

        public int inventorySize() {
            return inventorySize;
        }

        /** Number of IDs with a registered predicate (whether passing or failing). */
        public int activeCount() {
            return activeCount;
        }

        /** Number of IDs with no predicate yet (treated as passing shims). */
        public int shimmedCount() {
            return shimmedCount;
        }

        public List<String> failedIds() {
            return failedIds;
        }
    }
}
