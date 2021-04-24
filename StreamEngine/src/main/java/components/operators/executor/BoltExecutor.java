package components.operators.executor;
import common.collections.Configuration;
import components.context.TopologyContext;
import components.operators.api.Checkpointable;
import components.operators.api.Operator;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.Writer;
import common.Clock;
import db.DatabaseException;
import common.OrderLock;
import common.OrderValidate;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
public abstract class BoltExecutor implements IExecutor {
    private static final long serialVersionUID = 8641360612751721276L;
    private final Operator op;
    BoltExecutor(Operator op) {
        this.op = op;
    }
    public abstract void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        op.prepare(stormConf, context, collector);
    }
    public void loadDB(Configuration conf, TopologyContext context, OutputCollector collector) {
        op.loadDB(conf, context, collector);
    }
    @Override
    public int getID() {
        return op.getId();
    }
    @Override
    public double get_read_selectivity() {
        return op.read_selectivity;
    }
    @Override
    public Map<String, Double> get_input_selectivity() {
        return op.input_selectivity;
    }
    @Override
    public Map<String, Double> get_output_selectivity() {
        return op.output_selectivity;
    }
    @Override
    public double get_branch_selectivity() {
        return op.branch_selectivity;
    }
    @Override
    public String getConfigPrefix() {
        return op.getConfigPrefix();
    }
    @Override
    public TopologyContext getContext() {
        return op.getContext();
    }
    @Override
    public void display() {
        op.display();
    }
    @Override
    public double getResults() {
        return op.getResults();
    }
    @Override
    public double getLoops() {
        return op.loops;
    }
    @Override
    public boolean isScalable() {
        return op.scalable;
    }
    @Override
    public Integer default_scale(Configuration conf) {
        return op.default_scale(conf);
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
    @Override
    public void earlier_clean_state(Marker marker) {
    }
    @Override
    public void clean_state(Marker marker) {
        ((Checkpointable) op).ack_checkpoint(marker);
    }
    public int getStage() {
        return op.getFid();
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
        return 0;
    }
}
