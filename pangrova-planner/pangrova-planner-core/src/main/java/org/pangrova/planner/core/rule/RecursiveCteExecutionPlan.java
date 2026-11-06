package org.pangrova.planner.core.rule;

import org.pangrova.parser.api.ast.CteQuery.CycleClause;
import org.pangrova.parser.api.ast.CteQuery.SearchClause;
import org.pangrova.parser.api.ast.CteQuery.SearchOrder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
 * Iterative execution plan for recursive CTEs per SQL:1999.
 *
 * <p>Executes the seed query once, then iteratively applies the recursive step
 * to the working table until no new rows are produced or the iteration limit
 * is reached. Supports CYCLE clause for cycle detection and SEARCH clause
 * for controlling traversal order (depth-first or breadth-first).</p>
 *
 * <p>The execution model:
 * <ol>
 *   <li>Execute seed query → initial working table</li>
 *   <li>Loop: execute recursive step with current working table as input</li>
 *   <li>If CYCLE clause present, detect and mark cycles</li>
 *   <li>Append non-cyclic rows to result, replace working table with new rows</li>
 *   <li>Stop when working table is empty or iteration limit reached</li>
 * </ol>
 * </p>
 */
public final class RecursiveCteExecutionPlan {

    private final String cteName;
    private final RecursiveCteConfig config;
    private final Optional<SearchClause> searchClause;
    private final Optional<CycleClause> cycleClause;

    /**
     * Creates an execution plan for the named recursive CTE.
     *
     * @param cteName name of the recursive CTE
     * @param config recursion configuration (iteration limit, strategy)
     * @param searchClause optional SEARCH clause
     * @param cycleClause optional CYCLE clause
     */
    public RecursiveCteExecutionPlan(String cteName,
                                     RecursiveCteConfig config,
                                     Optional<SearchClause> searchClause,
                                     Optional<CycleClause> cycleClause) {
        this.cteName = cteName;
        this.config = config;
        this.searchClause = searchClause;
        this.cycleClause = cycleClause;
    }

    /**
     * Executes the recursive CTE using the iterative fixed-point algorithm.
     *
     * @param seedRows     rows produced by the seed (non-recursive) part
     * @param recursiveStep function that takes working table rows and produces new rows
     * @param <R>          row type
     * @return all accumulated result rows in traversal order
     */
    public <R> List<R> execute(List<R> seedRows, RecursiveStepFunction<R> recursiveStep) {
        List<R> result = new ArrayList<>(seedRows);
        Set<R> visited = cycleClause.isPresent() ? new HashSet<>(seedRows) : Set.of();

        if (searchClause.isPresent()
                && searchClause.get().order() == SearchOrder.DEPTH_FIRST) {
            return executeDepthFirst(seedRows, recursiveStep, result, visited);
        }
        return executeBreadthFirst(seedRows, recursiveStep, result, visited);
    }

    private <R> List<R> executeBreadthFirst(List<R> seedRows,
                                            RecursiveStepFunction<R> recursiveStep,
                                            List<R> result,
                                            Set<R> visited) {
        Queue<R> workingTable = new LinkedList<>(seedRows);
        int iteration = 0;

        while (!workingTable.isEmpty()) {
            iteration++;
            if (iteration > config.maxIterations()) {
                throw new RecursiveCteIterationLimitException(cteName, config.maxIterations());
            }

            List<R> currentBatch = new ArrayList<>(workingTable);
            workingTable.clear();

            List<R> newRows = recursiveStep.apply(currentBatch);
            appendRowsRespectingCycles(newRows, result, workingTable, visited);
        }
        return result;
    }

    /**
     * Appends each new row to {@code result} and {@code workingTable}, skipping
     * rows that have already been visited when a CYCLE clause is configured.
     * Extracted from {@link #executeBreadthFirst} to keep that method within
     * the Clean-Code indentation depth threshold.
     */
    private <R> void appendRowsRespectingCycles(List<R> newRows,
                                                List<R> result,
                                                Queue<R> workingTable,
                                                Set<R> visited) {
        boolean cycleAware = cycleClause.isPresent();
        for (R row : newRows) {
            if (cycleAware && !visited.add(row)) {
                continue;
            }
            result.add(row);
            workingTable.add(row);
        }
    }

    /**
     * Returns the subset of {@code newRows} that have not yet been visited,
     * marking each surviving row as visited when a CYCLE clause is configured.
     * Extracted from {@link #executeDepthFirst} to keep that method within
     * the Clean-Code indentation depth threshold.
     */
    private <R> List<R> collectNonCyclicChildren(List<R> newRows, Set<R> visited) {
        boolean cycleAware = cycleClause.isPresent();
        List<R> validChildren = new ArrayList<>(newRows.size());
        for (R row : newRows) {
            if (cycleAware && !visited.add(row)) {
                continue;
            }
            validChildren.add(row);
        }
        return validChildren;
    }

    private <R> List<R> executeDepthFirst(List<R> seedRows,
                                          RecursiveStepFunction<R> recursiveStep,
                                          List<R> result,
                                          Set<R> visited) {
        Deque<R> workingStack = new ArrayDeque<>();
        // Push seeds in reverse so first seed is processed first
        for (int i = seedRows.size() - 1; i >= 0; i--) {
            workingStack.push(seedRows.get(i));
        }
        int iteration = 0;

        while (!workingStack.isEmpty()) {
            iteration++;
            if (iteration > config.maxIterations()) {
                throw new RecursiveCteIterationLimitException(cteName, config.maxIterations());
            }

            R current = workingStack.pop();
            List<R> newRows = recursiveStep.apply(List.of(current));

            // Filter out cycles, collect valid children in forward order
            List<R> validChildren = collectNonCyclicChildren(newRows, visited);

            // Add to result in forward order (preserves DFS pre-order)
            result.addAll(validChildren);

            // Push to stack in reverse so first child is on top (processed next)
            for (int i = validChildren.size() - 1; i >= 0; i--) {
                workingStack.push(validChildren.get(i));
            }
        }
        return result;
    }

    public String cteName() {
        return cteName;
    }

    public RecursiveCteConfig config() {
        return config;
    }

    public Optional<SearchClause> searchClause() {
        return searchClause;
    }

    public Optional<CycleClause> cycleClause() {
        return cycleClause;
    }

    /**
     * Function that takes the current working table rows and produces new rows
     * by applying the recursive step of the CTE.
     */
    @FunctionalInterface
    public interface RecursiveStepFunction<R> {
        List<R> apply(List<R> workingTableRows);
    }
}
