package intellistream.morphstream.api.operator.spout.socket;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.socket.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.sink.socket.ApplicationSink;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.io.IOException;
import java.util.HashMap;
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
        ((ApplicationSink)sink).setSender(worker);
    }
    @Override
    public void nextTuple() throws InterruptedException {
        try {
            ZMsg msg = ZMsg.recvMsg(worker);
            if (msg != null) {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, msg, System.nanoTime());
                tuple = new Tuple(this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter ++;
                if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                    if (model_switch(counter)) {
                        marker = new Tuple(this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, counter, myiteration, "punctuation"));
                        bolt.execute(marker);
                    }
                }
            }
            } catch (BrokenBarrierException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (DatabaseException ex) {
            throw new RuntimeException(ex);
        }
    }
}
