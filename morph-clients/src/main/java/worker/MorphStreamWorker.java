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
    private final ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private final ZMQ.Socket backend;// Backend socket talks to workers over inproc

    public MorphStreamWorker(int numTasks) throws Exception {
        this.numTasks = numTasks;
        this.frontend = env.zContext().createSocket(SocketType.ROUTER);
        frontend.bind("tcp://*:5570");
        backend = env.zContext().createSocket(SocketType.DEALER);
        backend.bind("inproc://backend");
        this.spout = new FunctionExecutor("functionExecutor");
    }
    public void registerFunction(HashMap<String, FunctionDescription> functions) {
        this.spout.registerFunction(functions);
    }
    public boolean LoadConfiguration(String configPath, String[] args) throws IOException {
        if (configPath != null) {
            env.jCommanderHandler().loadProperties(configPath);
        }
        JCommander cmd = new JCommander(env.jCommanderHandler());
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            if (enable_log) LOG.error("Argument error: " + ex.getMessage());
            cmd.usage();
            return false;
        }
        env.jCommanderHandler().initializeCfg(env.configuration());
        return true;
    }

    public void prepare() throws IOException {
        env.DatabaseInitialize();
        if (env.configuration().getInt("inputSourceType", 0) == 0) { //read input as string
            String inputFile = env.configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
                env.fileDataGenerator().prepareInputData(true);
            } else {
                String fileName = env.fileDataGenerator().prepareInputData(false);
                env.configuration().put("inputFilePath", fileName);
            }
            if (env.fileDataGenerator().getTranToDecisionConf() != null && env.fileDataGenerator().getTranToDecisionConf().size() != 0){
                StringBuilder stringBuilder = new StringBuilder();
                for(String decision:env.fileDataGenerator().getTranToDecisionConf()){
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
                env.configuration().put("WorkloadConfig",stringBuilder.toString()); //For each workload, how many TD/LD/PD
            }
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_STRING, MorphStreamEnv.get().configuration().getInt("clientNum"));
        } else if (env.configuration().getInt("inputSourceType", 0) == 1) { //read input as JSON
            String inputFile = env.configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
            } else {
                String fileName = env.fileDataGenerator().prepareInputData(false);
                env.configuration().put("inputFilePath", fileName);
            }
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_JSON, MorphStreamEnv.get().configuration().getInt("clientNum"));
        }
        env.CountDownLatchInitialize(MorphStreamEnv.get().configuration().getInt("clientNum") + 1); //+1 for MorphStreamWorker
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
        env.latch().countDown();
        ZMQ.proxy(frontend, backend, null);//Connect backend to frontend via a proxy
    }
}
