package worker.rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.spout.rdma.FunctionExecutor;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class MorphStreamWorker extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamWorker.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final FunctionExecutor spout;
    private final int numTasks;
    private final int workerId;
    private final RdmaWorkerManager rdmaWorkerManager;

    public MorphStreamWorker() throws Exception {
        workerId = env.configuration().getInt("workerId", 0);
        this.numTasks = env.configuration().getInt("tthread", 1);
        this.rdmaWorkerManager = new RdmaWorkerManager(false, env.configuration());
        this.spout = new FunctionExecutor("functionExecutor", rdmaWorkerManager);
        LOG.info("MorphStreamWorker: " + env.configuration().getInt("workerId", 0) +" is initialized");
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
    }
}
