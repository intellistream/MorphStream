package worker.rdma;

import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MorphStreamDriver extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamDriver.class);
    private ZContext zContext = new ZContext();
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc\
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final CountDownLatch workerLatch;
    private final int numFrontend;
    private final List<Thread> frontends = new ArrayList<>();
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
        statistic = new Statistic(MorphStreamEnv.get().configuration().getInt("workerNum",4), MorphStreamEnv.get().configuration().getInt("shuffleType", 0));
        rdmaDriverManager = new RdmaDriverManager(true, env.configuration(), statistic);
        workerLatch = MorphStreamEnv.get().workerLatch();
    }
    public void initialize() throws IOException {
        for (int i = 0; i < numFrontend; i++) {
            Thread frontend = new Thread(new MorphStreamFrontend(i, zContext, rdmaDriverManager, statistic));
            frontends.add(frontend);
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
        }
        MorphStreamEnv.get().clientLatch().countDown();
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
}
