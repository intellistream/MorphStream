package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.api.AbstractBolt;
import intellistream.morphstream.engine.stream.components.operators.api.AbstractSpout;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static intellistream.morphstream.configuration.CONTROL.*;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private final MorphStreamEnv env = MorphStreamEnv.get();
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
        // initialize AppConfig
        //AppConfig.complexity = conf.getInt("complexity", 100000);
        //AppConfig.windowSize = conf.getInt("windowSize", 1024);
        //AppConfig.isCyclic = conf.getBoolean("isCyclic", true);
        //if (CONTROL.enable_shared_state) {
        //    Metrics.COMPUTE_COMPLEXITY = conf.getInt("COMPUTE_COMPLEXITY");
        //    Metrics.POST_COMPUTE_COMPLEXITY = conf.getInt("POST_COMPUTE");
        //    Metrics.NUM_ACCESSES = conf.getInt("NUM_ACCESS");
        //    Metrics.NUM_ITEMS = conf.getInt("NUM_ITEMS");
        //    Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        //}
        return true;
    }

    public void prepare() throws IOException {
        env.DatabaseInitialize();
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
        runTopologyLocally();
        //TODO: run for distributed mode
    }
    public void setSpout(String id, AbstractSpout spout, int numTasks) {
        env.setSpout(id, spout, numTasks);
    }
    public void setBolt(String id, AbstractBolt bolt, int numTasks, Grouping ... groups) {
        env.setBolt(id, bolt, numTasks, groups);
    }
    public void setSink(String id, AbstractBolt sink, int numTasks, Grouping ... groups) {
        env.setBolt(id, sink, numTasks, groups);
    }


    private void runTopologyLocally() throws InterruptedException {
        Topology topology = env.createTopology();
        env.submitTopology(topology);
        listenToStop();
        //TODO: stop application
    }

    public void listenToStop() throws InterruptedException {
        executorThread sinkThread = env.OM().getEM().getSinkThread();
        sinkThread.join((long) (30 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins

    }

    public MorphStreamEnv env() {
        return env;
    }
}
