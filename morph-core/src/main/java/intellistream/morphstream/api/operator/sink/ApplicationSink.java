package intellistream.morphstream.api.operator.sink;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.delete.BaseSink;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends AbstractSink {
    public ApplicationSink(Logger log, int fid) {
        super(log, fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

    }

    @Override
    public DescriptiveStatistics getLatencyStats() {
        return null;
    }

    @Override
    public double getThroughputStats() {
        return 0;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

    }
}
