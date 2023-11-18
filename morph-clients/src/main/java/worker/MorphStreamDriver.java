package worker;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

public class MorphStreamDriver extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamDriver.class);
    private ZContext zContext = new ZContext();
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc\
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final int numFrontend;
    private final List<Thread> frontends = new ArrayList<>();
    public MorphStreamDriver() {
        this.numFrontend = env.configuration().getInt("frontendNum");
        frontend = zContext.createSocket(SocketType.ROUTER);//  Frontend socket talks to clients over TCP
        String address = env.configuration().getString("morphstream.socket.driverHost");
        int port = env.configuration().getInt("morphstream.socket.driverPort");
        frontend.bind("tcp://"+ address +":" + port);
        backend = zContext.createSocket(SocketType.DEALER); //  Backend socket talks to workers over inproc
        backend.bind("inproc://backend");
    }
    public void initialize() {
        for (int i = 0; i < numFrontend; i++) {
            Thread frontend = new Thread(new MorphStreamFrontend(zContext));
            frontends.add(frontend);
            //Connect Worker use RDMA
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < numFrontend; i++) {
            frontends.get(i).start();
        }
        MorphStreamEnv.get().latch().countDown();
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
}
