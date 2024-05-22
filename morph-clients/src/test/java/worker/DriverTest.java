package worker;

import intellistream.morphstream.api.input.statistic.Statistic;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import worker.rdma.MorphStreamDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DriverTest extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamDriver.class);
    private ZContext zContext = new ZContext();
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc\
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final int numFrontend;
    private final List<FrontendTest> frontends = new ArrayList<>();

    private Statistic statistic;
    public DriverTest() throws Exception {
        this.numFrontend = env.configuration().getInt("frontendNum");
        frontend = zContext.createSocket(SocketType.ROUTER);//  Frontend socket talks to clients over TCP
        String address = "localhost";
        int port = 5557;
        frontend.bind("tcp://"+ address +":" + port);
        backend = zContext.createSocket(SocketType.DEALER); //  Backend socket talks to workers over inproc
        backend.bind("inproc://backend");
        statistic = new Statistic(MorphStreamEnv.get().configuration().getInt("workerNum",4), MorphStreamEnv.get().configuration().getInt("shuffleType", 0), MorphStreamEnv.get().configuration().getString("tableNames","table1,table2").split(","));
    }
    public void initialize() {
        for (int i = 0; i < numFrontend; i++) {
            frontends.add(new FrontendTest(i, zContext, statistic));
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < numFrontend; i++) {
            frontends.get(i).start();
        }
        MorphStreamEnv.get().clientLatch().countDown();
        LOG.info("Driver is running.");
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
    public void stopDriver() {
        for (int i = 0; i < numFrontend; i++) {
            while (frontends.get(i).isRunning()) {
                frontends.get(i).interrupt();
            }
        }
        zContext.close();
    }
    public void display(){
        this.statistic.display();
    }
}
