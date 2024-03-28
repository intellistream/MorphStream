package worker.rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.spout.rdma.FunctionExecutor;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import lombok.Getter;
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
    @Getter
    private final RdmaWorkerManager rdmaWorkerManager;

    public MorphStreamWorker() throws Exception {
        workerId = env.configuration().getInt("workerId", 0);
        this.numTasks = env.configuration().getInt("tthread", 1);
        this.rdmaWorkerManager = new RdmaWorkerManager(false, env.configuration());
        this.spout = new FunctionExecutor("functionExecutor");
        LOG.info("MorphStreamWorker: " + env.configuration().getInt("workerId", 0) +" is initialized");
    }
    public void initialize(HashMap<String, FunctionDescription> functions) throws DatabaseException {
        this.registerFunction(functions);
        MorphStreamEnv.get().DatabaseInitialize();
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runTopologyLocally() throws Exception {
        Topology topology = env.createTopology();
        env.submitTopology(topology);
        this.rdmaWorkerManager.connectDriver();
        env.OM().start();
    }

}
