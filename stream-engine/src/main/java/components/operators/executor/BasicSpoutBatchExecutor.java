package components.operators.executor;

import components.context.TopologyContext;
import components.operators.api.AbstractSpout;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;

import java.util.Map;

public class BasicSpoutBatchExecutor extends SpoutExecutor {
    private static final long serialVersionUID = 5741034817930924249L;
    private final AbstractSpout _op;

    public BasicSpoutBatchExecutor(AbstractSpout op) {
        super(op);
        this._op = op;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _op.prepare(stormConf, context, collector);
    }

    @Override
    public int getID() {
        return _op.getId();
    }

    @Override
    public String getConfigPrefix() {
        return _op.getConfigPrefix();
    }

    @Override
    public TopologyContext getContext() {
        return _op.getContext();
    }

    @Override
    public void display() {
        _op.display();
    }

    @Override
    public double getResults() {
        return _op.getResults();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void callback(int callee, Marker marker) {
        _op.callback(callee, marker);
    }

    public void bulk_emit(int batch) throws InterruptedException {
        for (int i = 0; i < batch; i++) {
            _op.nextTuple();
        }
    }

    public void setExecutionNode(ExecutionNode executionNode) {
        _op.setExecutionNode(executionNode);
    }
}