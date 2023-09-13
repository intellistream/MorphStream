package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.topology.TopologySubmitter;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.exception.UnhandledCaseException;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.engine.txn.utils.SINK_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.METRICS_REPORT;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.METRICS_REPORT_WITH_FAILURE;
import static intellistream.morphstream.engine.txn.profiler.Metrics.timer;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private static Topology final_topology;
    public static CliFrontend getOrCreate() {
        return new CliFrontend();
    }
    public CliFrontend appName(String appName) {
        this.appName = appName;
        return this;
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
        //TODO: add other configs, initializeCfg(config); // initialize AppConfig (TopologySubmitter)
        //TODO: add metric config
        return true;
    }

    public void prepare() throws IOException {
        //TODO:initialize Database and configure input and output
        env.databaseInitialize().creates_Table();
        String inputFile = env.configuration().getString("inputFile");
        File file = new File(inputFile);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
        } else {
            String fileName = env.fileDataGenerator().prepareInputData();
            env.configuration().put("inputFile", fileName);
        }
    }
    public void run() throws InterruptedException {
//        LoadConfiguration(); //TODO: Read from config.inputFile and set config accordingly, and store to env?

//        AppDriver.AppDescriptor app = driver.getApp(application);
//        Topology topology = app.getTopology(application, config);
//        topology.addMachine(platform);
//        Topology topology = env.transactionalTopology().builder.createTopology();

        runTopologyLocally(); //Class Topology, Configuration
        //TODO: run for distributed mode
        
    }

    private static void runTopologyLocally() throws InterruptedException {
        TopologySubmitter submitter = new TopologySubmitter(); //TODO: replace with env? initialize OM, EM, etc.
        try {
            final_topology = submitter.submitTopology();
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();
        sinkThread.join((long) (30 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins
        //TODO: stop application
        submitter.getOM().join();
        submitter.getOM().getEM().exist();
    }

    public void stop() {
    }

    public MorphStreamEnv env() {
        return env;
    }
}
