package engine.stream.components.operators.executor;

import engine.stream.components.operators.api.Operator;
import engine.stream.execution.ExecutionNode;

public abstract class SpoutExecutor implements IExecutor {
    private static final long serialVersionUID = -6394372792803974178L;
    private final Operator op;

    SpoutExecutor(Operator op) {
        this.op = op;
    }

    public void setExecutionNode(ExecutionNode e) {
        op.setExecutionNode(e);
    }

    public int getStage() {
        return op.getFid();
    }

    public double getEmpty() {
        return op.getEmpty();
    }
}