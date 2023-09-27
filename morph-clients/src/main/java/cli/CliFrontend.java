package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.spout.ApplicationSpout;
import intellistream.morphstream.api.operator.spout.ApplicationSpoutCombo;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.CCOption_MorphStream;
import static intellistream.morphstream.configuration.Constants.CCOption_SStore;

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
            } else {
                String fileName = env.fileDataGenerator().prepareInputData();
                env.configuration().put("inputFilePath", fileName);
            }
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_STRING, env.configuration().getInt("spoutNum"));
        } else if (env.configuration().getInt("inputSourceType", 0) == 1) { //read input as JSON
            String inputFile = env.configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
            } else {
                String fileName = env.fileDataGenerator().prepareInputData();
                env.configuration().put("inputFilePath", fileName);
            }
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_JSON, env.configuration().getInt("spoutNum"));
        }
    }
    public void run() throws InterruptedException {
//        MeasureTools.Initialize();
        runTopologyLocally();
        //TODO: run for distributed mode
    }
    public void setSpoutCombo(String id, HashMap<String, TxnDescription> txnDescriptionHashMap, int numTasks) throws Exception {
        ApplicationSpoutCombo spout = new ApplicationSpoutCombo(txnDescriptionHashMap);
        env.setSpout(id, spout, numTasks);
    }
    public void setSpout(String id, int numTasks) throws Exception {
        ApplicationSpout spout = new ApplicationSpout();
        env.setSpout(id, spout, numTasks);
    }
    public void setBolt(String id, HashMap<String, TxnDescription> txnDescriptionHashMap, int numTasks, int fid, Grouping ... groups) {
        AbstractBolt bolt = null;
        switch (env.configuration().getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(txnDescriptionHashMap, fid);
                break;
            }
            case CCOption_SStore:{
                bolt = new SStoreBolt(txnDescriptionHashMap, fid);
                break;
            }
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
        env.setBolt(id, bolt, numTasks, groups);
    }
    public void setSink(String id, AbstractBolt sink, int numTasks, int fid, Grouping ... groups) {
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
        env.OM().join();
        env.OM().getEM().exist();
    }

    public MorphStreamEnv env() {
        return env;
    }
}
