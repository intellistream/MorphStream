package intellistream.morphstream.api.operator.spout.rdma;

import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.rdma.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.sink.rdma.ApplicationSink;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.RdmaWorkerManager;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

public class FunctionExecutor extends AbstractSpoutCombo {
    private static final Logger LOG = LoggerFactory.getLogger(FunctionExecutor.class);
    private String operatorID;
    private HashMap<String, FunctionDescription> FunctionDescriptionHashMap;
    private Configuration conf = MorphStreamEnv.get().configuration();
    private ByteBuffer msgBuffer;
    private RdmaWorkerManager rdmaWorkerManager;
    public FunctionExecutor(String operatorID, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        super(operatorID, LOG, 0);
        this.operatorID = operatorID;
        this.rdmaWorkerManager = rdmaWorkerManager;
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
        ((ApplicationSink)sink).setSender(rdmaWorkerManager);
    }
    @Override
    public void nextTuple() throws InterruptedException {
        try {
            byte[] msg = getMsg();
            if (msg != null) {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, InputSource.inputFromByteToTxnEvent(msg), System.nanoTime());
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
            } catch (BrokenBarrierException | IOException | DatabaseException ex) {
            throw new RuntimeException(ex);
        }
    }
    private byte[] getMsg() throws IOException {
        if (!msgBuffer.hasRemaining()) {
            ByteBuffer address = rdmaWorkerManager.getCircularRdmaBuffer().canRead();
            int length = address.getInt();
            if (length == 0) {
                return null;
            } else {
                List<Integer> lengthQueue = new ArrayList<>();
                lengthQueue.add(length);
                while(address.hasRemaining()) {
                    lengthQueue.add(address.getInt());
                }
                long myOffset = 0;
                int myLength = lengthQueue.get(this.threadId);
                for (int i = 0; i < this.threadId; i++) {
                    myOffset += lengthQueue.get(i);
                }
                msgBuffer = rdmaWorkerManager.getCircularRdmaBuffer().read(myOffset, myLength);
            }
        }
        int length1 = msgBuffer.getInt();
        byte[] bytes1 = new byte[length1];
        return bytes1;
    }
}
