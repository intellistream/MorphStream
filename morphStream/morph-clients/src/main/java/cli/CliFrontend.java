package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import commonStorage.RequestTemplates;
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

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private MorphStreamEnv env;

    private final HashMap<String, AbstractBolt> boltMap = new HashMap<>();
    private final HashMap<String, String[]> stateObjectTemplates = new HashMap<>();
    private int counter = 0;
    private final int punctuation_interval = MorphStreamEnv.get().configuration().getInt("checkpoint", 2500);
    private final int ccOption = MorphStreamEnv.get().configuration().getInt("CCOption", 0);

    public CliFrontend(String appName) throws IOException {
        this.appName = appName;
        env = MorphStreamEnv.get();
    }

    public void loadConfigStreaming(String[] args) throws IOException {
        try {
            LoadConfiguration(null, args);
            prepareStreaming();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        env.initializeAdaptiveCCManager(); //Separate initialization of adaptive CC manager from MorphStreamEnv constructor
    }

    public static double getDoubleField(String stateObjID, String[] txnData) {
        String saID = txnData[0]; //saId determines which saNameToIndex map to refer to
        int readFieldIndex = RequestTemplates.saDataNameToIndex.get(saID).get(stateObjID);
        return Double.parseDouble(txnData[readFieldIndex]);
    }

    public static void setDoubleField(String stateObjID, double value, String[] saData) {
        saData[2] = Double.toString(value);
    }

    public static void abortTxn(String[] txnData) {
        txnData[1] = "true";
    }

    public void registerStateObject(String stateObjID, String stateID, int keyIndexInEvent, int fieldTableIndex, String type) {
        stateObjectTemplates.put(stateObjID, new String[]{stateID, Integer.toString(keyIndexInEvent), Integer.toString(fieldTableIndex), type});
    }

    public void registerStateAccess(String stateAccessID, String[] stateObjectIDs, String[] valueNames, String type) {

        String[] stateAccessTemplate = new String[3 + stateObjectIDs.length * 4]; //saID, saType, writeKeyIndex, N*[tableName, keyIndex, fieldIndex, saType]
        stateAccessTemplate[0] = stateAccessID;
        stateAccessTemplate[1] = type;
        RequestTemplates.saDataNameToIndex.put(stateAccessID, new HashMap<>());
        int templateIndex = 3;
        int saIndex = 3; //indexes in saData, the first 3 are: saID, txnAbortFlag, saResult, the rest are all stateObject fields read from table

        for (String stateObjectID : stateObjectIDs) {
            String[] stateObjectTemplate = stateObjectTemplates.get(stateObjectID);
            stateAccessTemplate[templateIndex] = stateObjectTemplate[0]; //table name
            stateAccessTemplate[templateIndex + 1] = stateObjectTemplate[1]; //key index in event
            stateAccessTemplate[templateIndex + 2] = stateObjectTemplate[2]; //field index in table
            stateAccessTemplate[templateIndex + 3] = stateObjectTemplate[3]; //state access type
            RequestTemplates.saDataNameToIndex.get(stateAccessID).put(stateObjectID, saIndex);
            if (stateObjectTemplate[3].equals("WRITE")) {
                stateAccessTemplate[2] = String.valueOf(templateIndex); //indicating this state object is to be written during state access
            }
            templateIndex += 4;
            saIndex++;
        }
        RequestTemplates.sharedSATemplates.put(stateAccessID, stateAccessTemplate);
    }

    public void registerTxn(String txnID, String[] stateAccessIDs) {
        RequestTemplates.sharedTxnTemplates.put(txnID, stateAccessIDs);
    }

    /**
     * Register a new operator to the system. This combines both operator (VNF) creation and topology node registration
    * */
    public void registerOperator(String operatorID, String[] txnIDs, int stage, int parallelism) {
        RequestTemplates.sharedOperatorTemplates.put(operatorID, txnIDs);
        try {
//            AbstractBolt bolt = setBolt(operatorID, parallelism, stage);
//            boltMap.put(operatorID, bolt); //TODO: Extract bolt logic from SASpout for optimization
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


    public void prepareStreaming() {
        env.DatabaseInitialize();
//        env.inputSource().initializeStreaming(MorphStreamEnv.get().configuration().getInt("spoutNum"));
    }

    public void start() throws InterruptedException {
//        MeasureTools.Initialize();
        runTopologyLocally();
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
        env.submitTopology(topology);
        listenToStop();
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
