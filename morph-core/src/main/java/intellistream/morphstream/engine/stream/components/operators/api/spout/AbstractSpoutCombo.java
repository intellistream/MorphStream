package intellistream.morphstream.engine.stream.components.operators.api.spout;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractTransactionalBolt;
import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

public abstract class AbstractSpoutCombo extends AbstractSpout {
    protected AbstractTransactionalBolt bolt;
    protected AbstractSink sink;

    public AbstractSpoutCombo(String id, Logger log, int i) {
        super(id, log, i);
        this.scalable = false;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        bolt.loadDB(conf, context, collector);
    }
}
