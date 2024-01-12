package worker.rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Memory.CircularRdmaBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MorphStreamFrontend extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamFrontend.class);
    private int threadId;
    private ZMQ.Socket frontend;// Frontend socket talks to Driver over TCP
    private RdmaDriverManager rdmaDriverManager;
    private List<Integer> workIdList = new ArrayList<>();
    protected int sendCount = 0;
    protected int receiveCount = 0;
    private Tuple2<Long, ByteBuffer> tempCanRead;//the temp buffer to decide whether the result buffer can read
    private HashMap<Integer, ByteBuffer> workerIdToResultBufferMap = new HashMap<>();//the map to store the result buffer that can read
    private ConcurrentHashMap<Integer, CircularRdmaBuffer> workerIdToCircularRdmaBufferMap = new ConcurrentHashMap<>();//the map to store all result buffer
    private Statistic statistic;
    private String tempInput;
    private TransactionalEvent tempEvent;
    public MorphStreamFrontend(int threadId, ZContext zContext, RdmaDriverManager rdmaDriverManager, Statistic statistic) {
        this.frontend = zContext.createSocket(SocketType.DEALER);
        frontend.connect("inproc://backend");
        this.rdmaDriverManager = rdmaDriverManager;
        workerIdToCircularRdmaBufferMap = rdmaDriverManager.getRdmaBufferManager().getResultBufferMap();
        this.statistic = statistic;
        String[] workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        this.threadId = threadId;
        for (int i = 0; i < workerHosts.length; i++) {
            workIdList.add(i);
        }
    }

    public void asyncReceiveFunctionOutput() throws IOException {
        ByteBuffer results = getResult();
        if (getResult() != null && results.hasRemaining()) {
            int length = results.getInt();
            byte[] bytes = new byte[length];
            results.get(bytes);
            String result = new String(bytes);
            receiveCount ++;
            System.out.println(result);
        }
    }
    public void invokeFunctionToWorker(int workId) throws Exception {
        rdmaDriverManager.send(workId, new FunctionMessage(tempInput));
        sendCount ++;
    }

    public void run(){
        while (!interrupted()) {
            ZMsg msg = ZMsg.recvMsg(frontend, false);
            if (msg != null) {
                try {
                    tempInput = msg.getLast().toString();
                    tempEvent = InputSource.inputFromStringToTxnEvent(tempInput);
                    invokeFunctionToWorker(getWorkId(tempEvent.getAllKeys()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                asyncReceiveFunctionOutput();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private int getWorkId(List<String> keys) {
        return this.statistic.add(keys);
    }
    private ByteBuffer getResult() throws IOException {
        if (hasRemaining() == -1) {
            for (int i = 0; i < workerIdToCircularRdmaBufferMap.size(); i++) {
                tempCanRead = workerIdToCircularRdmaBufferMap.get(i).canRead(this.threadId);
                int length = tempCanRead._2.getInt();
                if (length != 0) {
                    List<Integer> lengthQueue = new ArrayList<>();
                    lengthQueue.add(length);
                    while(tempCanRead._2.hasRemaining()) {
                        lengthQueue.add(tempCanRead._2.getInt());
                    }
                    long myOffset = 4L * threadId + tempCanRead._1();
                    int myLength = lengthQueue.get(this.threadId);
                    for (int j = 0; j < this.threadId; j++) {
                        myOffset += lengthQueue.get(i);
                    }
                    ByteBuffer byteBuffer = workerIdToCircularRdmaBufferMap.get(i).read(myOffset, myLength);
                    workerIdToResultBufferMap.put(i, byteBuffer);
                }
            }
            if (hasRemaining() == -1)
                return null;
        }
        return workerIdToResultBufferMap.get(hasRemaining());
    }
    private int hasRemaining() {
        for (int i = 0; i < workerIdToResultBufferMap.size(); i++) {
            if (workerIdToResultBufferMap.get(i) != null && workerIdToResultBufferMap.get(i).hasRemaining()) {
                return i;
            }
        }
        return -1;
    }
}
