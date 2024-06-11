package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.sink.ApplicationSink;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

public class SACombo extends AbstractSpoutCombo {
    private static final Logger LOG = LoggerFactory.getLogger(SACombo.class);
    private String operatorID;
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public SACombo(String operatorID) throws Exception {
        super(operatorID, LOG, 0);
        this.operatorID = operatorID;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink = new ApplicationSink("sink", 0);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(operatorID, 0, this.sink);
                break;
            }
            case CCOption_SStore:
                bolt = new SStoreBolt(operatorID, 0, this.sink);
                break;
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
                break;
        }
        bolt.prepare(conf, context, collector);
//        bolt.loadDB(conf, context, collector);
    }

    @Override
    public void nextTuple() throws InterruptedException {
        try {
            if (!inputQueue.isEmpty()) {
                TransactionalEvent event = inputQueue.take(); //this should be txnEvent already
                long bid = event.getBid();

                if (bid != -1) { //txn events
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
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "punctuation"));
                            bolt.execute(marker);
                        }
                    }
                } else { //stop signal arrives, stop the current spout thread
                    System.out.println("TPG thread " + this.taskId + " received stop signal.");
                    SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                    getContext().stop_running(); //stop itself
                }
            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
