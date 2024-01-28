package intellistream.morphstream.engine.stream.components.operators.executor;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.Operator;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.db.exception.DatabaseException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public abstract class BoltExecutor implements IExecutor {
    private static final long serialVersionUID = 8641360612751721276L;
    private final Operator op;

    BoltExecutor(Operator op) {
        this.op = op;
    }

    public abstract void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;

    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;

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

    public void setExecutionNode(ExecutionNode e) {
        op.setExecutionNode(e);
    }

    public int getStage() {
        return op.getFid();
    }

    public double getEmpty() {
        return 0;
    }
}
