package worker.rdma;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.spout.rdma.FunctionExecutor;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDAGDescription;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
        MorphStreamEnv.get().setRdmaWorkerManager(getRdmaWorkerManager());
        this.spout = new FunctionExecutor("functionExecutor");
        LOG.info("MorphStreamWorker: " + env.configuration().getInt("workerId", 0) +" is initialized");
    }
    public void initialize() throws DatabaseException {
        String clientName = MorphStreamEnv.get().configuration().getString("clientClassName");
        try {
            Class<?> clazz = Class.forName(clientName);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            Client t = (Client) instance;
            t.defineFunction();
            this.registerFunction(t.txnDescriptions);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        MorphStreamEnv.get().DatabaseInitialize();
        MeasureTools.Initialize(MorphStreamEnv.get().configuration());
    }
    public void registerFunction(HashMap<String, FunctionDAGDescription> functions) {
        this.spout.registerFunction(functions);
    }

    @Override
    public void run() {
        env.setSpout("functionExecutor", spout, numTasks);
        try {
            runTopologyLocally();
            this.RdmaWorkerManagerJoin();
            LOG.info("MorphStreamWorker: " + env.configuration().getInt("workerId", 0) +" is finished");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void RdmaWorkerManagerJoin() throws InterruptedException {
        MorphStreamEnv.get().OM().getEM().ThreadMap.get(0).join();
        boolean allFinished = false;
        while (!allFinished) {
            allFinished = true;
            for (int i = 0; i < MorphStreamEnv.get().OM().getEM().ThreadMap.size(); i++) {
                if (MorphStreamEnv.get().OM().getEM().ThreadMap.get(i).running) {
                    allFinished = false;
                    break;
                }
            }
            Thread.sleep(1000);
        }
        MorphStreamEnv.get().rdmaWorkerManager().close();
        MeasureTools.WORKER_METRICS_REPORT(numTasks, MorphStreamEnv.get().configuration().getString("scheduler"));
        System.exit(0);
    }

    private void runTopologyLocally() throws Exception {
        Topology topology = env.createTopology();
        env.submitTopology(topology);
        this.rdmaWorkerManager.connectDatabase();
        this.rdmaWorkerManager.connectDriver();
        env.OM().start();
    }

}
