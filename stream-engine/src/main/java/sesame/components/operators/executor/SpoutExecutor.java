package sesame.components.operators.executor;

import sesame.components.operators.api.Checkpointable;
import sesame.components.operators.api.Operator;
import state_engine.Clock;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.faulttolerance.Writer;
import state_engine.common.OrderLock;
import state_engine.common.OrderValidate;

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

    public void configureLocker(OrderLock lock, OrderValidate orderValidate) {
        op.lock = lock;
        op.orderValidate = orderValidate;
    }

    public int getStage() {
        return op.getFid();
    }

    @Override
    public void clean_state(Marker marker) {
        ((Checkpointable) op).ack_checkpoint(marker);
    }

    @Override
    public void earlier_clean_state(Marker marker) {
        ((Checkpointable) op).earlier_ack_checkpoint(marker);
    }

    public boolean IsStateful() {
        return op.IsStateful();
    }

    public void forceStop() {
        op.forceStop();
    }


    public void setclock(Clock clock) {
        this.op.clock = clock;
    }


    public double getEmpty() {
        return op.getEmpty();
    }
}