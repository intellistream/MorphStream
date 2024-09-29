package intellistream.morphstream.api;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Client extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private final Map<String, ZMQ.Socket> sockets = new HashMap<>();
    public HashMap<String, FunctionDAGDescription> txnDescriptions = new HashMap<>(); //Flag -> TxnDescription
    public boolean isRunning = true;
    protected final ZContext zContext = new ZContext();
    protected BlockingQueue<TransactionalEvent> inputQueue;
    protected int clientId;
    protected String clientIdentity;
    protected ZMQ.Poller poller;
    protected CountDownLatch latch;
    protected int msgCount = 0;
    private String gatewayHost;
    private int gatewayPort;
    private int qps = 0;
    public abstract boolean transactionUDF(Function function);
    public abstract Result postUDF(long bid, String txnFlag, HashMap<String, Function> FunctionMap);
    public abstract void defineFunction();
    public ZMQ.Socket getSocket(String address) {
        return sockets.getOrDefault(address, null);
    }
    public void connectFrontend(String address, int driverPort) {
        ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
        clientIdentity = String.format("%04X-%04X", ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt());
        socket.setIdentity(clientIdentity.getBytes(ZMQ.CHARSET));
        socket.connect("tcp://" + address + ":" + driverPort);
        sockets.put(address, socket);
        poller = zContext.createPoller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);
        LOG.info("Client {} connects gateway.", clientId);
    }
    public void initialize(int clientId, CountDownLatch latch) throws IOException {
        this.clientId = clientId;
        this.qps = MorphStreamEnv.get().configuration().getInt("qps");
        LOG.info("Client {} is initialized.", clientId);
        this.latch = latch;
    }
    public void asyncInvokeFunction(String driverName, String function) {
        getSocket(driverName).send(function.getBytes());
    }
    public void asyncReceiveFunctionOutput(String workerName) {
        for (int centitick = 0; centitick < 100; centitick++) {
            poller.poll(0);
            if (poller.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(getSocket(workerName));
                LOG.info("Receive: " + msg.popString());
                msgCount ++;
                msg.destroy();
            }
        }
    }

    @Override
    public void run() {
        this.inputQueue = MorphStreamEnv.get().inputSource().getInputQueue(clientId);
        gatewayHost = MorphStreamEnv.get().configuration().getString("gatewayHost");
        gatewayPort = MorphStreamEnv.get().configuration().getInt("gatewayPort");

        connectFrontend(gatewayHost, gatewayPort);
        latch.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        while (!Thread.currentThread().isInterrupted()) {
            if (!inputQueue.isEmpty()) {
                asyncInvokeFunction("localhost", inputQueue.poll().toString());
            }
            //asyncReceiveFunctionOutput("localhost");
        }
        this.close();
        this.isRunning = false;
    }
    public void close() {
        for (ZMQ.Socket socket : sockets.values()) {
            socket.close();
        }
        zContext.close();
    }
}
