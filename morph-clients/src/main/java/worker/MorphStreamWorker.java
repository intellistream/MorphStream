package worker;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.spout.FunctionExecutor;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.HashMap;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class MorphStreamWorker extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamWorker.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc\
    private final FunctionExecutor spout;
    private final int numTasks;
    private final int workerId;

    public MorphStreamWorker() throws Exception {
        workerId = env.configuration().getInt("workerId", 0);
        this.numTasks = env.configuration().getInt("tthread", 1);
        this.spout = new FunctionExecutor("functionExecutor");
        String[] workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.socket.workerHosts").split(",");
        String[] workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.socket.workerPorts").split(",");
        frontend = env.zContext().createSocket(SocketType.ROUTER);//  Frontend socket talks to clients over TCP
        frontend.bind("tcp://" + workerHosts[workerId] + ":" + workerPorts[workerId]);
        backend = env.zContext().createSocket(SocketType.DEALER); //  Backend socket talks to workers over inproc
        backend.bind("inproc://backend");
        LOG.info("MorphStreamWorker: " + env.configuration().getInt("workerId", 0) +" is initialized, listening on " + "tcp://" + workerHosts[workerId] + ":" + workerPorts[workerId]);

    }
    public void registerFunction(HashMap<String, FunctionDescription> functions) {
        this.spout.registerFunction(functions);
    }
    @Override
    public void run() {
        env.setSpout("functionExecutor", spout, numTasks);
        MeasureTools.Initialize();
        try {
            runTopologyLocally();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void runTopologyLocally() throws InterruptedException {
        Topology topology = env.createTopology();
        env.submitTopology(topology);
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
}
