package components.operators.executor;
import common.collections.Configuration;
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
    public double get_read_selectivity() {
        return _op.read_selectivity;
    }
    @Override
    public Map<String, Double> get_input_selectivity() {
        return _op.input_selectivity;
    }
    @Override
    public Map<String, Double> get_output_selectivity() {
        return _op.output_selectivity;
    }
    @Override
    public double get_branch_selectivity() {
        return _op.branch_selectivity;
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
    public double getLoops() {
        return _op.getLoops();
    }
    @Override
    public boolean isScalable() {
        return _op.scalable;
    }
    @Override
    public Integer default_scale(Configuration conf) {
        return _op.default_scale(conf);
    }
    @Override
    public void cleanup() {
    }
    @Override
    public void callback(int callee, Marker marker) {
        _op.callback(callee, marker);
    }
    public void bulk_emit_nonblocking(int batch) throws InterruptedException {
        for (int i = 0; i < batch; i++) {
            _op.nextTuple_nonblocking();
        }
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