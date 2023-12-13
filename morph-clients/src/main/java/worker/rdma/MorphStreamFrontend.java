package worker.rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Memory.CircularRdmaBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MorphStreamFrontend extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamFrontend.class);
    private int threadId;
    private int totalThread;
    private ZMQ.Socket frontend;// Frontend socket talks to Driver over TCP
    private RdmaDriverManager rdmaDriverManager;
    private List<Integer> workIdList = new ArrayList<>();
    private Random random = new Random();
    private int shuffleType = 0;
    private int currentId = 0;
    protected int sendCount = 0;
    protected int receiveCount = 0;
    private HashMap<Integer, ByteBuffer> workerIdToResultBufferMap = new HashMap<>();
    public MorphStreamFrontend(int threadId, ZContext zContext, RdmaDriverManager rdmaDriverManager) {
        this.frontend = zContext.createSocket(SocketType.DEALER);
        frontend.connect("inproc://backend");
        this.rdmaDriverManager = rdmaDriverManager;
        String[] workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        this.shuffleType = MorphStreamEnv.get().configuration().getInt("shuffleType", 0);
        this.totalThread = MorphStreamEnv.get().configuration().getInt("frontendNum");
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
    public void invokeFunctionToWorker(int workId, ZMsg msg) throws Exception {
        String input = msg.getLast().toString();
        rdmaDriverManager.send(workId, new FunctionMessage(input));
        sendCount++;
    }

    public void run(){
        while (!Thread.currentThread().interrupted()) {
            ZMsg msg = ZMsg.recvMsg(frontend, false);
            if (msg != null) {
                try {
                    invokeFunctionToWorker(getWorkId(this.shuffleType), msg);
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
    private int getWorkId(int shuffleType) {
        switch (shuffleType) {
            case 0://Sort
                return 0;
            case 1://Random
                return 1;
            case 2://Optimized
                return 2;
            default:
               throw new RuntimeException("Wrong shuffle type!");
        }
    }
    private int getNextWorkIdSort(){
        if (currentId == workIdList.size()) {
            currentId = 0;
        }
        return workIdList.get(currentId++);
    }
    private int getNextWorkIdRandom(){
       return random.nextInt(workIdList.size());
    }
    private int getNextWorkIdOptimized(){
        //TODO: Implement optimized shuffle
        return 0;
    }
    private ByteBuffer getResult() throws IOException {
        if (hasRemaining() == -1) {
            ConcurrentHashMap<Integer, CircularRdmaBuffer> workerIdToCircularRdmaBufferMap = rdmaDriverManager.getRdmaBufferManager().getResultBufferMap();
            for (int i = 0; i < workerIdToCircularRdmaBufferMap.size(); i++) {
               ByteBuffer address = workerIdToCircularRdmaBufferMap.get(i).canRead();
               int length = address.getInt();
               if (length != 0) {
                   List<Integer> lengthQueue = new ArrayList<>();
                   lengthQueue.add(length);
                   while(address.hasRemaining()) {
                       lengthQueue.add(address.getInt());
                   }
                   long myOffset = 0;
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
            if (workerIdToResultBufferMap.get(i).hasRemaining()) {
                return i;
            }
        }
        return -1;
    }
}
