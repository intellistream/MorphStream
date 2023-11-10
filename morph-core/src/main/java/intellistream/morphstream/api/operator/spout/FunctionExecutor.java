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
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

public class FunctionExecutor extends AbstractSpoutCombo {
    private static final Logger LOG = LoggerFactory.getLogger(FunctionExecutor.class);
    private String operatorID;
    private ZMQ.Socket worker;
    private HashMap<String, FunctionDescription> FunctionDescriptionHashMap;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public FunctionExecutor(String operatorID) throws Exception {
        super(operatorID, LOG, 0);
        this.operatorID = operatorID;
    }
    public void registerFunction(HashMap<String, FunctionDescription> functions) {
        this.FunctionDescriptionHashMap = functions;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink = new ApplicationSink("sink", 0);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(operatorID, FunctionDescriptionHashMap, 0, this.sink);
                break;
            }
            case CCOption_SStore:
                bolt = new SStoreBolt(operatorID, FunctionDescriptionHashMap, 0, this.sink);
                break;
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
                break;
        }
        bolt.prepare(conf, context, collector);
        bolt.loadDB(conf, context, collector);
        worker = MorphStreamEnv.get().zContext().createSocket(SocketType.DEALER);
        worker.connect("inproc://backend");
    }
    @Override
    public void nextTuple() throws InterruptedException {
        ZMsg msg = ZMsg.recvMsg(worker);
        ZFrame address = msg.pop();
        ZFrame content = msg.pop();
        assert (content != null);
        msg.destroy();
        address.send(worker, ZFrame.REUSE + ZFrame.MORE);
        content.send(worker, ZFrame.REUSE);
        address.destroy();
        content.destroy();
//        try {
//            if (!inputQueue.isEmpty()) {
//                TransactionalEvent event = inputQueue.take(); //this should be txnEvent already
//                long bid = event.getBid();
//
//                if (bid != -1) { //txn events
//                    if (CONTROL.enable_latency_measurement)
//                        generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
//                    else {
//                        generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//                    }
//
//                    tuple = new Tuple(bid, this.taskId, context, generalMsg);
//                    bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//                    counter++;
//
//                    if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                        if (model_switch(counter)) {
//                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "punctuation"));
//                            bolt.execute(marker);
//                        }
//                    }
//                } else { //control signals
//                    if (Objects.equals(event.getFlag(), "pause")) {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "pause"));
//                        bolt.execute(marker);
//                    }
//                }
//
////                if (inputQueue.isEmpty()) { //TODO: Refactor this part, remove the_end, use stopEvent indicator
////                    SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
////                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
////                    getContext().stop_running();
////                }
//            }
//        } catch (DatabaseException | BrokenBarrierException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
}
