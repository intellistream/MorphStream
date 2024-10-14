package intellistream.morphstream.api;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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
    protected HashMap<Long, Long> bidToEventTime = new HashMap<>();
    public DescriptiveStatistics latencyStats = new DescriptiveStatistics();
    public long startTime;
    public double throughput;
    protected int clientId;
    protected String clientIdentity;
    protected ZMQ.Poller poller;
    protected CountDownLatch latch;
    protected int msgToReceived = 0;
    private String gatewayHost;
    private int gatewayPort;
    private int qps = 0;
    private long interval = 0;
    public abstract boolean transactionUDF(Function function);
    public abstract Result postUDF(long bid, String txnFlag, HashMap<String, Function> FunctionMap);
    public abstract void defineFunction();
    public ZMQ.Socket getSocket(String address) {
        return sockets.getOrDefault(address, null);
    }
    public void connectFrontend(String address, int driverPort) {
        ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
        clientIdentity = String.format("%04X-%04X", ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt());
        this.qps = MorphStreamEnv.get().configuration().getInt("qps") / MorphStreamEnv.get().configuration().getInt("clientNum");
        this.interval = 1000_000_000L / qps;//ns
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
    public void asyncInvokeFunction(String driverName, String function, long bid) {
        getSocket(driverName).send(function.getBytes());
        this.bidToEventTime.put(bid, System.nanoTime());
    }
    public void asyncReceiveFunctionOutput(String workerName) {
        poller.poll(interval / 1_000_000 / 2);
        if (poller.pollin(0)) {
            ZMsg msg = ZMsg.recvMsg(getSocket(workerName));
            long bid = Long.parseLong(msg.popString());
            this.latencyStats.addValue((double) (System.nanoTime() - this.bidToEventTime.get(bid)) / 1_000_000);
            msgToReceived --;
            msg.destroy();
        }
    }

    @Override
    public void run() {
        this.inputQueue = MorphStreamEnv.get().inputSource().getInputQueue(clientId);
        this.msgToReceived = this.inputQueue.size();
        gatewayHost = MorphStreamEnv.get().configuration().getString("gatewayHost");
        gatewayPort = MorphStreamEnv.get().configuration().getInt("gatewayPort");

        connectFrontend(gatewayHost, gatewayPort);
        latch.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        long lastSentTime = System.nanoTime();
        long startTime = System.nanoTime();
        while (msgToReceived > 0) {
            asyncReceiveFunctionOutput("localhost");
            if (System.nanoTime() - lastSentTime > interval) {
                if (!inputQueue.isEmpty()) {
                    lastSentTime = System.nanoTime();
                    TransactionalEvent event = inputQueue.poll();
                    asyncInvokeFunction("localhost", event.toString(), event.getBid());
                }
            }
        }
        throughput = (double) this.latencyStats.getN() / (System.nanoTime() - startTime) * 1_000_000_000;
        this.close();
        LOG.info("Client {} is closed.", clientId);
        this.isRunning = false;
    }
    public void close() {
        for (ZMQ.Socket socket : sockets.values()) {
            socket.close();
        }
        zContext.close();
    }
}
