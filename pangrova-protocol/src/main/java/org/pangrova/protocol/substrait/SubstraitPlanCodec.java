package org.pangrova.protocol.substrait;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.Plan;

/**
 * Parses serialized Substrait plans with an explicit recursion budget.
 *
 * <p>Protobuf defaults to 100 nested messages, which rejects legitimate plans
 * whose chained subqueries exceed that depth (for example a recursive CTE the
 * planner resolves into levels). The limit stays bounded so adversarial input
 * cannot exhaust the stack.</p>
 */
public final class SubstraitPlanCodec {

    /** Nested-message budget for coordinator, planner and worker plan parsing. */
    public static final int RECURSION_LIMIT = 1024;

    private SubstraitPlanCodec() {}

    /** Parses a Substrait plan with a bounded recursion limit. */
    public static Plan parse(byte[] bytes) throws InvalidProtocolBufferException {
        try {
            CodedInputStream input = CodedInputStream.newInstance(bytes);
            input.setRecursionLimit(RECURSION_LIMIT);
            return Plan.parseFrom(input);
        } catch (InvalidProtocolBufferException failure) {
            throw failure;
        } catch (java.io.IOException failure) {
            throw new InvalidProtocolBufferException(failure.getMessage());
        }
    }
}
