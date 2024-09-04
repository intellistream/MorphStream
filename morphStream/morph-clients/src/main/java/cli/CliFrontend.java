package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import commonStorage.RequestTemplates;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.spout.ApplicationSpout;
import intellistream.morphstream.api.operator.spout.ApplicationSpoutCombo;
import intellistream.morphstream.api.operator.spout.SACombo;
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
import static intellistream.morphstream.configuration.Constants.*;


public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private MorphStreamEnv env;

    public CliFrontend(String appName) {
        this.appName = appName;
        env = MorphStreamEnv.get();
    }

    public void loadConfigStreaming(String[] args) {
        try {
            LoadConfiguration(null, args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void initializeDB() {
        env.DatabaseInitialize();
    }

    public void prepareAdaptiveCC() {
        env.initializeAdaptiveCCManager();
    }

    public void registerStateAccess(String saID, String saType, String tableName) {
        String[] saTemplate = new String[3]; //saID, saType, tableName
        saTemplate[0] = saID;
        saTemplate[1] = saType;
        saTemplate[2] = tableName;
        RequestTemplates.sharedSATemplates.put(saID, saTemplate);
    }

    public void registerTxn(String txnID, String[] stateAccessIDs) {
        RequestTemplates.sharedTxnTemplates.put(txnID, stateAccessIDs);
    }

    /**
     * Register a new operator to the system. This combines both operator (VNF) creation and topology node registration
     * */
    public void registerOperator(String operatorID, int parallelism) {
        try {
            SACombo operator = new SACombo(operatorID);
            env.setSpout(operatorID, operator, parallelism);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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


    public void runStateManager() throws InterruptedException {
        String ccStrategy = env.configuration().getString("ccStrategy");
        boolean enableMemoryFootprint = (MorphStreamEnv.get().configuration().getInt("enableMemoryFootprint") == 1);
        if (ccStrategy == "Partitioning") {
            env.getAdaptiveCCManager().startPartitionCC();
            startVNF();
            env.getAdaptiveCCManager().joinPartitionCC();
        } else if (ccStrategy == "Replication") {
            env.getAdaptiveCCManager().startReplicationCC();
            startVNF();
            env.getAdaptiveCCManager().joinReplicationCC();
        } else if (ccStrategy == "Offloading") {
            env.getAdaptiveCCManager().startOffloadExecutorThreads();
            startVNF();
            env.getAdaptiveCCManager().joinOffloadExecutorThreads();
        } else if (ccStrategy == "Proactive") {
            runTopologyLocally();
        } else if (ccStrategy == "OpenNF") {
            env.getAdaptiveCCManager().startOpenNF();
            startVNF();
            env.getAdaptiveCCManager().joinOpenNF();
        } else if (ccStrategy == "CHC") {
            env.getAdaptiveCCManager().startCHC();
            startVNF();
            env.getAdaptiveCCManager().joinCHC();
        } else if (ccStrategy == "S6") {
            env.getAdaptiveCCManager().startS6();
            startVNF();
            env.getAdaptiveCCManager().joinS6();
        } else if (ccStrategy == "Adaptive") {
            env.getAdaptiveCCManager().startAdaptiveCC();
            runTopologyLocally();
            env.getAdaptiveCCManager().joinAdaptiveCC();
        } else {
            if (enable_log) LOG.error("Unknown CC strategy: " + ccStrategy);
        }
        if (enableMemoryFootprint) {
            StateManagerRunner.stopMemoryMonitoring();
        }
    }

    public void setSpoutCombo(String id, HashMap<String, TxnDescription> txnDescriptionHashMap, int numTasks) throws Exception {
        ApplicationSpoutCombo spout = new ApplicationSpoutCombo(id, txnDescriptionHashMap);
        env.setSpout(id, spout, numTasks);
    }
    public void setSpout(String id, int numTasks) throws Exception {
        ApplicationSpout spout = new ApplicationSpout(id);
        env.setSpout(id, spout, numTasks);
    }
    public AbstractBolt setBolt(String id, int numTasks, int fid, Grouping ... groups) {
        AbstractBolt bolt = null;
        switch (env.configuration().getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(id, fid);
                break;
            }
            case CCOption_SStore:{
                bolt = new SStoreBolt(id, fid);
                break;
            }
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
        env.setBolt(id, bolt, numTasks, groups);
        return bolt;
    }
    public void setSink(String id, AbstractBolt sink, int numTasks, int fid, Grouping ... groups) {
        env.setBolt(id, sink, numTasks, groups);
    }

    private void runTopologyLocally() throws InterruptedException {
        Topology topology = env.createTopology();
        env.submitTopology(topology); //This starts the TPG_CC threads (MorphStreamBolts)
        listenToStop();
    }

    public void listenToStop() throws InterruptedException {
        executorThread sinkThread = env.OM().getEM().getSinkThread();
        startVNF();
        sinkThread.join((long) (30 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins
        env.OM().join();
        env.OM().getEM().exist();
    }

    public MorphStreamEnv env() {
        return env;
    }

    private static void startVNF() {
        writeIndicatorFile("manager_ready");
        Thread vnfThread = new Thread(new VNFManager());
        vnfThread.start();
        LOG.info("VNF instances have started.");
    }

    private static void writeIndicatorFile(String fileName) {
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String directoryPath = rootPath + "/indicators";
        String filePath = String.format("%s/%s.csv", directoryPath, fileName);
        LOG.info("Writing indicator: " + fileName);

        File dir = new File(directoryPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return; // Stop further processing if unable to create the directory
            }
        }

        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }

        try {
            file.createNewFile();
        } catch (IOException e) {
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
    }

}
