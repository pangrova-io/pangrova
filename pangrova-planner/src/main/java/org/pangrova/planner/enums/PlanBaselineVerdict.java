package org.pangrova.planner.enums;

/**
 * Result of comparing the observed query plan with its registered baseline.
 *
 * <ul>
 *   <li>{@link #MATCH} — a baseline exists and the observed plan matches.</li>
 *   <li>{@link #MISMATCH} — a baseline exists and the observed plan
 *       differs (operator should triage).</li>
 *   <li>{@link #NO_BASELINE} — no baseline registered for this query.</li>
 * </ul>
 */
public enum PlanBaselineVerdict { MATCH, MISMATCH, NO_BASELINE }
