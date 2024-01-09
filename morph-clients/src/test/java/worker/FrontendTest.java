package worker;

import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
public class FrontendTest extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(FrontendTest.class);
    private boolean isRunning = true;
    private int threadId;
    private ZMQ.Socket frontend;// Frontend socket talks to Driver over TCP
    private List<Integer> workIdList = new ArrayList<>();
    private Random random = new Random();
    private int currentId = 0;
    protected int sendCount = 0;
    protected int receiveCount = 0;
    private Statistic statistic;
    private String tempInput;
    private TransactionalEvent tempEvent;
    public FrontendTest(int threadId, ZContext zContext, Statistic statistic) {
        this.frontend = zContext.createSocket(SocketType.DEALER);
        frontend.connect("inproc://backend");
        String[] workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        this.threadId = threadId;
        for (int i = 0; i < workerHosts.length; i++) {
            workIdList.add(i);
        }
        this.statistic = statistic;
        LOG.info("Frontend " + threadId + " is initialized.");
    }

    public void invokeFunctionToWorker(int workId, ZMsg msg) throws Exception {
        sendCount ++;
    }

    public void run(){
        LOG.info("Frontend " + threadId + " is running.");
        while (!Thread.currentThread().interrupted() && isRunning) {
            ZMsg msg = ZMsg.recvMsg(frontend, false);
            if (msg != null) {
                try {
                    tempInput = msg.getLast().toString();
                    tempEvent = InputSource.inputFromStringToTxnEvent(tempInput);
                    invokeFunctionToWorker(getWorkId(tempEvent.getAllKeys()), msg);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        isRunning = false;
    }
    public boolean isRunning() {
        return isRunning;
    }
    private int getWorkId(List<String> keys) {
        return this.statistic.add(keys);
    }
}
