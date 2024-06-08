package intellistream.morphstream.api.operator.spout.rdma;

import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.rdma.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.bolt.rdma.RemoteStorageBolt;
import intellistream.morphstream.api.operator.sink.rdma.ApplicationSink;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutCombo;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

public class FunctionExecutor extends AbstractSpoutCombo {
    private static final Logger LOG = LoggerFactory.getLogger(FunctionExecutor.class);
    private String operatorID;
    private HashMap<String, FunctionDAGDescription> FunctionDescriptionHashMap;
    private Configuration conf = MorphStreamEnv.get().configuration();
    private ByteBuffer msgBuffer;
    private int eventCount = 0;
    private Tuple2<Long, ByteBuffer> canRead;
    public FunctionExecutor(String operatorID) throws Exception {
        super(operatorID, LOG, 0);
        this.operatorID = operatorID;
    }
    public void registerFunction(HashMap<String, FunctionDAGDescription> functions) {
        this.FunctionDescriptionHashMap = functions;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink = new ApplicationSink("sink", 0);
        sink.prepare(conf, context, collector);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(operatorID, FunctionDescriptionHashMap, 0, this.sink);
                break;
            }
            case CCOption_RemoteLock: {//Distributed
                bolt = new RemoteStorageBolt(operatorID, FunctionDescriptionHashMap, 0, this.sink);
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
    }
    @Override
    public void nextTuple() throws InterruptedException {
        try {
            MeasureTools.WorkerRdmaRecvStartEventTime(threadId);
            byte[] msg = getMsg();
            MeasureTools.WorkerRdmaRecvEndEventTime(threadId);
            if (msg != null) {
                MeasureTools.WorkerPrepareStartTime(threadId);
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, InputSource.inputFromByteToTxnEvent(msg), System.nanoTime());
                tuple = new Tuple(this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                eventCount ++;
                MeasureTools.WorkerPrepareEndTime(threadId);
                if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore || ccOption == CCOption_RemoteLock) {// This is only required by T-Stream.
                    if (model_switch(counter) && !msgBuffer.hasRemaining()) {
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
        if (msgBuffer == null || !msgBuffer.hasRemaining()) {
            canRead = MorphStreamEnv.get().rdmaWorkerManager().getCircularRdmaBuffer().canRead(this.threadId);
            if (canRead != null) {
                List<Integer> lengthQueue = new ArrayList<>();
                while(canRead._2().hasRemaining()) {
                    lengthQueue.add(canRead._2().getInt());
                }
                long myOffset = canRead._1();
                int myLength = lengthQueue.get(this.threadId);
                for (int i = 0; i < this.threadId; i++) {
                    myOffset += lengthQueue.get(i);
                }
                if (myLength != 0) {
                    msgBuffer = MorphStreamEnv.get().rdmaWorkerManager().getCircularRdmaBuffer().read(myOffset, myLength);
                    counter ++;
                } else {
                    context.stop_running();
                    return null;
                }
            } else {
                return null;
            }
        }
        int length1 = msgBuffer.getInt();
        byte[] bytes1 = new byte[length1];
        msgBuffer.get(bytes1);
        return bytes1;
    }
}
