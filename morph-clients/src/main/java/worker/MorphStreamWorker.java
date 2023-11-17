package worker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.spout.FunctionExecutor;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.FunctionDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import static intellistream.morphstream.configuration.CONTROL.*;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class MorphStreamWorker extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamWorker.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final FunctionExecutor spout;
    private final int numTasks;

    public MorphStreamWorker() throws Exception {
        this.numTasks = env.configuration().getInt("tthread", 1);
        this.spout = new FunctionExecutor("functionExecutor");
    }
    public void registerFunction(HashMap<String, FunctionDescription> functions) {
        this.spout.registerFunction(functions);
    }

    public void prepare() throws IOException {
        //To connect Driver and Database
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
        //TODO: run for distributed mode
    }

    private void runTopologyLocally() throws InterruptedException {
        Topology topology = env.createTopology();
        env.submitTopology(topology);
    }
}
