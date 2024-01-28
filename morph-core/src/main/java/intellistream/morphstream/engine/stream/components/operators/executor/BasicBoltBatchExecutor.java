package intellistream.morphstream.engine.stream.components.operators.executor;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.db.DatabaseException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class BasicBoltBatchExecutor extends BoltExecutor {
    private static final long serialVersionUID = 5928745739657994175L;
    private final AbstractBolt _op;

    public BasicBoltBatchExecutor(AbstractBolt op) {
        super(op);
        _op = op;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        _op.execute(in);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        _op.execute(in);
    }
}
