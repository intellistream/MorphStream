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

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

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

    public void createTransNFVStateManager() {
        env.createTransNFVStateManager();
    }

    public void prepareTransNFVStateManager() {
        String ccStrategy = env.configuration().getString("ccStrategy");

        if (Objects.equals(ccStrategy, "Partitioning")) {
            env.getTransNFVStateManager().preparePartitionStateManager();
        } else if (Objects.equals(ccStrategy, "Replication")) {
            env.getTransNFVStateManager().prepareReplicationStateManager();
        } else if (Objects.equals(ccStrategy, "Offloading")) {
            env.getTransNFVStateManager().prepareOffloadExecutors();
        } else if (Objects.equals(ccStrategy, "Proactive")) {
            env.getTransNFVStateManager().prepareProactiveExecutors();
        } else if (Objects.equals(ccStrategy, "OpenNF")) {
            env.getTransNFVStateManager().prepareOpenNFStateManager();
        } else if (Objects.equals(ccStrategy, "CHC")) {
            env.getTransNFVStateManager().prepareCHCStateManager();
        } else if (Objects.equals(ccStrategy, "S6")) {
            env.getTransNFVStateManager().prepareS6StateManager();
        } else if (Objects.equals(ccStrategy, "Adaptive")) {
            //TODO: Prepare adaptive state manager
            env.getTransNFVStateManager().prepareBatchWorkloadMonitor();
        } else {
            if (enable_log) LOG.error("Unknown CC strategy: " + ccStrategy);
        }
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


    public void startTransNFVStateManager() throws InterruptedException {
        String ccStrategy = env.configuration().getString("ccStrategy");
        boolean enableMemoryFootprint = (MorphStreamEnv.get().configuration().getInt("enableMemoryFootprint") == 1);
        if (Objects.equals(ccStrategy, "Partitioning")) {
            env.getTransNFVStateManager().startPartitionStateManager();
            startVNF();
            env.getTransNFVStateManager().joinPartitionStateManager();
        } else if (Objects.equals(ccStrategy, "Replication")) {
            env.getTransNFVStateManager().startReplicationStateManager();
            startVNF();
            env.getTransNFVStateManager().joinReplicationStateManager();
        } else if (Objects.equals(ccStrategy, "Offloading")) {
            env.getTransNFVStateManager().startOffloadExecutors();
            startVNF();
            env.getTransNFVStateManager().joinOffloadExecutors();
        } else if (Objects.equals(ccStrategy, "Proactive")) {
            runTopologyLocally(); // This starts the TPG threads
        } else if (Objects.equals(ccStrategy, "OpenNF")) {
            env.getTransNFVStateManager().startOpenNFStateManager();
            startVNF();
            env.getTransNFVStateManager().joinOpenNFStateManager();
        } else if (Objects.equals(ccStrategy, "CHC")) {
            env.getTransNFVStateManager().startCHCStateManager();
            startVNF();
            env.getTransNFVStateManager().joinCHCStateManager();
        } else if (Objects.equals(ccStrategy, "S6")) {
            env.getTransNFVStateManager().startS6StateManager();
            startVNF();
            env.getTransNFVStateManager().joinS6StateManager();
        } else if (Objects.equals(ccStrategy, "Adaptive")) {
            env.getTransNFVStateManager().startAdaptiveCC();
            runTopologyLocally();
            env.getTransNFVStateManager().joinAdaptiveCC();
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
        Thread vnfThread = new Thread(new VNFManager());
        vnfThread.start();
        LOG.info("VNF instances have started.");
    }

}
