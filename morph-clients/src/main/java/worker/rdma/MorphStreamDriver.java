package worker.rdma;

import client.BankingSystemClient;
import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaDriverManager;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class MorphStreamDriver extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamDriver.class);
    private ZContext zContext = new ZContext();
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc\
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final CountDownLatch workerLatch;
    private final int numFrontend;
    private final List<MorphStreamFrontend> frontends = new ArrayList<>();
    private final RdmaDriverManager rdmaDriverManager;
    private final Statistic statistic;
    public MorphStreamDriver() throws Exception {
        this.numFrontend = env.configuration().getInt("frontendNum");
        frontend = zContext.createSocket(SocketType.ROUTER);//  Frontend socket talks to clients over TCP
        String address = "localhost";
        int port;
        if (MorphStreamEnv.get().configuration().getBoolean("isRDMA")) {
            port = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        } else {
            port = MorphStreamEnv.get().configuration().getInt("morphstream.socket.driverPort");
        }
        frontend.bind("tcp://"+ address +":" + port);
        backend = zContext.createSocket(SocketType.DEALER); //  Backend socket talks to workers over inproc
        backend.bind("inproc://backend");
        statistic = new Statistic(MorphStreamEnv.get().configuration().getInt("workerNum",4), MorphStreamEnv.get().configuration().getInt("shuffleType", 0), MorphStreamEnv.get().configuration().getString("tableNames","table1,table2").split(";"), this.numFrontend);
        rdmaDriverManager = new RdmaDriverManager(true, env.configuration(), statistic);
        workerLatch = MorphStreamEnv.get().workerLatch();
    }
    public void initialize() throws IOException {
        MeasureTools.Initialize(MorphStreamEnv.get().configuration());
        for (int i = 0; i < numFrontend; i++) {
            frontends.add(new MorphStreamFrontend(i, zContext, rdmaDriverManager, statistic));
        }
        MorphStreamEnv.get().InputSourceInitialize();
    }

    @Override
    public void run() {
        try {
            workerLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < numFrontend; i++) {
            frontends.get(i).start();
            frontends.get(i).setSystemStartTime(System.nanoTime());
        }
        MorphStreamEnv.get().clientLatch().countDown();
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
    public void startClient() {
        String clientName = MorphStreamEnv.get().configuration().getString("clientClassName");
        int clientNum = MorphStreamEnv.get().configuration().getInt("clientNum");
        List<Thread> threads = new ArrayList<>();
        for (int threadNum = 0; threadNum < clientNum; threadNum++) {
            try {
                Class<?> clazz = Class.forName(clientName);
                Object instance = clazz.getDeclaredConstructor().newInstance();
                Client t = (Client) instance;
                t.initialize(threadNum, MorphStreamEnv.get().clientLatch());
                threads.add(t);
                t.start();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                     InvocationTargetException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void MorphStreamDriverJoin() {
        try {
            this.frontends.get(0).join();
            boolean allFinished = false;
            while (!allFinished) {
                allFinished = true;
                for (MorphStreamFrontend morphStreamFrontend : frontends) {
                    if (morphStreamFrontend.isRunning) {
                        allFinished = false;
                        break;
                    }
                }
                Thread.sleep(1000);
            }
            this.rdmaDriverManager.close();
            LOG.info("MorphStreamDriver is finished with throughput: " + statistic.getThroughput() + " k DAGs/s");
            LOG.info("MorphStreamDriver is finished with average latency: " + statistic.getLatency() + " ms");
            LOG.info("MorphStreamDriver is finished with 99th latency: " + statistic.getLatency(99) + " ms");
            MeasureTools.DRIVER_METRICS_REPORT(numFrontend, statistic.getThroughput(), statistic.getLatencyStatistics());
            System.exit(0);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
