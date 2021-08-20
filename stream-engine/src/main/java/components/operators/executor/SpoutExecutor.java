package components.operators.executor;

import components.operators.api.Checkpointable;
import components.operators.api.Operator;
import execution.ExecutionNode;
import execution.runtime.tuple.impl.Marker;
import faulttolerance.Writer;
import lock.Clock;

public abstract class SpoutExecutor implements IExecutor {
    private static final long serialVersionUID = -6394372792803974178L;
    private final Operator op;

    SpoutExecutor(Operator op) {
        this.op = op;
    }

    public void setExecutionNode(ExecutionNode e) {
        op.setExecutionNode(e);
    }

    public void configureWriter(Writer writer) {
        if (op.state != null) {
            op.state.writer = writer;
        }
    }

    public int getStage() {
        return op.getFid();
    }

    @Override
    public void clean_state(Marker marker) {
        ((Checkpointable) op).ack_checkpoint(marker);
    }

    public void setclock(Clock clock) {
        this.op.clock = clock;
    }

    public double getEmpty() {
        return op.getEmpty();
    }
}