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

    protected int ccOption;
    public int taskId;
    public AbstractSpoutCombo(Logger log, int i) {
        super(log, i);
        this.scalable = false;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        ccOption = config.getInt("CCOption", 0);
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        bolt.loadDB(conf, context, collector);
    }

    @Override
    public void nextTuple() throws InterruptedException {
        try {
            if (counter == start_measure) {
                //TODO: start sink
            }
            if (!inputQueue.isEmpty()) {

                //TODO: InputSource inputSource should be retrieved from client.getInputSource();
                //TODO: Should we keep both streaming and original input?

                TransactionalEvent event = inputQueue.take(); //this should be txnEvent already

                long bid = event.getBid();
                if (CONTROL.enable_latency_measurement)
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
                else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                    if (model_switch(counter)) {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                        bolt.execute(marker);
                    }
                }

                if (inputQueue.isEmpty()) { //TODO: Refactor this part, remove the_end, use stopEvent indicator
                    SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                    //TODO: stop sink
                }
            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
