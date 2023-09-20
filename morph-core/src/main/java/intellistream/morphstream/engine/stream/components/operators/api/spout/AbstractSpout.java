package intellistream.morphstream.engine.stream.components.operators.api.spout;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.Checkpointable;
import intellistream.morphstream.engine.stream.components.operators.api.Operator;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractSpout extends Operator implements Checkpointable {
    private static final long serialVersionUID = -7455539617930687503L;
    public int punctuation_interval;
    protected int totalEventsPerBatch = 0;
    protected int tthread;
    protected BlockingQueue<TransactionalEvent> inputQueue;
    protected int start_measure = 0;
    protected int counter = 0;

    public GeneralMsg generalMsg;
    public Tuple tuple;
    public Tuple marker;
    protected int myiteration = 0;//start from 1st iteration.
    protected AbstractSpout(Logger log, int fid) {
        super(log, 1);
        this.fid = fid;
    }
    public void nextTuple() throws InterruptedException{
        //TODO
    }
    @Override
    public boolean model_switch(int counter) {
        return (counter % punctuation_interval == 0);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        punctuation_interval = config.getInt("checkpoint");
        totalEventsPerBatch = config.getInt("totalEvents");
        tthread = config.getInt("tthread");
    }
    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        throw new UnsupportedOperationException();
    }
    public BlockingQueue<TransactionalEvent> getInputQueue() {
        return inputQueue;
    }
}
