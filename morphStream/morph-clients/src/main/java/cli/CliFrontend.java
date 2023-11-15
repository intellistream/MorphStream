package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.MorphStreamBolt;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.spout.ApplicationSpout;
import intellistream.morphstream.api.operator.spout.ApplicationSpoutCombo;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObjectDescription;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 * TODO: This class should be the receiving end of system, it waits for new app from clients, and perform system initialization.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final HashMap<String, StateObjectDescription> stateObjectMap = new HashMap<>();
    private final HashMap<String, StateAccessDescription> stateAccessMap = new HashMap<>();
    private final HashMap<String, TxnDescription> txnMap = new HashMap<>();
    private final HashMap<String, AbstractBolt> operatorMap = new HashMap<>();
    /**
     * For each StateAccess, saDataNameToIndex maps name to index in saData array
     * "saID": 0
     * "bid": 1
     * "<stateObjName>": <index> *N
     * "<perEventValueName>": <index> *N
     */
    public static HashMap<String, HashMap<String, Integer>> saDataNameToIndex = new HashMap<>();
    private int counter = 0;
    private final int punctuation_interval = MorphStreamEnv.get().configuration().getInt("checkpoint", 2500);
    private final int ccOption = MorphStreamEnv.get().configuration().getInt("CCOption", 0);

    public CliFrontend(String appName) {
        this.appName = appName;
    }

    public void loadConfig() {
        try {
            LoadConfiguration(null, null);
            prepare();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadConfig(String configPath, String[] args) throws IOException {
        try {
            LoadConfiguration(configPath, args);
            prepare();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static double getDoubleField(String stateObjID, String[] txnData) {
        String saID = txnData[0]; //saId determines which saNameToIndex map to refer to
        int readFieldIndex = saDataNameToIndex.get(saID).get(stateObjID);
        return Double.parseDouble(txnData[readFieldIndex]);
    }

    public static void setDoubleField(String stateObjID, double value, String[] txnData) {
        String saID = txnData[0];
        int writeFieldIndex = saDataNameToIndex.get(saID).get(stateObjID);
        txnData[writeFieldIndex] = Double.toString(value);
    }

    public void registerStateObject(String stateObjID, String stateID, int keyIndexInEvent, int fieldTableIndex, String type) {
        MetaTypes.AccessType accessType;
        if (type.equals("READ")) {
            accessType = MetaTypes.AccessType.READ;
        } else if (type.equals("WRITE")) {
            accessType = MetaTypes.AccessType.WRITE;
        } else {
            throw new RuntimeException("Invalid access type");
        }
        StateObjectDescription stateObjectDescription = new StateObjectDescription(stateObjID, accessType, stateID, keyIndexInEvent, fieldTableIndex); //index specifies the index of key in the input event
        stateObjectMap.put(stateObjID, stateObjectDescription);
    }

    public void registerStateAccess(String stateAccessID, String[] stateObjectIDs, String[] valueNames, String type) {
        MetaTypes.AccessType accessType;
        if (type.equals("READ")) {
            accessType = MetaTypes.AccessType.READ;
        } else {
            accessType = MetaTypes.AccessType.WRITE;
        }

        saDataNameToIndex.put(stateAccessID, new HashMap<>());
        saDataNameToIndex.get(stateAccessID).put("saID", 0);
        saDataNameToIndex.get(stateAccessID).put("bid", 1);
        int index = 2;

        StateAccessDescription stateAccessDescription = new StateAccessDescription(stateAccessID, accessType);
        for (String stateObjectID : stateObjectIDs) {
            stateAccessDescription.addStateObjectDescription(stateObjectMap.get(stateObjectID));
            saDataNameToIndex.get(stateAccessID).put(stateObjectID, index);
            index++;
        }
        for (String valueName : valueNames) {
            stateAccessDescription.addValueName(valueName);
            //TODO: Add value index into saDataNameToIndex later
        }
        stateAccessMap.put(stateAccessID, stateAccessDescription);
    }

    public void registerTxn(String txnID, String[] stateAccessIDs) {
        TxnDescription txnDescription = new TxnDescription();
        for (String stateAccessID : stateAccessIDs) {
            txnDescription.addStateAccess(stateAccessID, stateAccessMap.get(stateAccessID));
        }
        txnMap.put(txnID, txnDescription);
    }

    /**
     * Register a new operator to the system. This combines both operator (VNF) creation and topology node registration
    * */
    public void registerOperator(String operatorID, String[] txnIDs, int stage, int parallelism) {
        MorphStreamEnv.get().configuration().put("useNativeLib", true);
        HashMap<String, TxnDescription> txnDescriptionHashMap = new HashMap<>();

        for (String txnID : txnIDs) {
            txnDescriptionHashMap.put(txnID, txnMap.get(txnID));
        }
        try {
            AbstractBolt bolt = setBolt(operatorID, txnDescriptionHashMap, parallelism, stage);
            operatorMap.put(operatorID, bolt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sendTxnRequest(int bid, String operatorID, String txnFlag,
                               HashMap<String, List<String>> keyMap,
                               HashMap<String, Object> valueMap,
                               HashMap<String, String> valueTypeMap) {
        TransactionalEvent event = new TransactionalEvent(bid, keyMap, valueMap, valueTypeMap, txnFlag, false);
        AbstractBolt bolt = operatorMap.get(operatorID);
        GeneralMsg generalMsg;
        if (CONTROL.enable_latency_measurement)
            generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
        else {
            generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
        }

        //TODO: Initialize multiple txn request handler threads and identify them using sourceID below
        Tuple tuple = new Tuple(bid, 0, null, generalMsg); //tuple.context is useless everywhere
        try {
            bolt.execute(tuple);
            counter++;
            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {
                if (counter % punctuation_interval == 0) {
                    Tuple marker = new Tuple(bid, 0, null, new Marker(DEFAULT_STREAM_ID, -1, bid, 0, "punctuation")); //myIteration is always 0
                    bolt.execute(marker);
                }
            }
        } catch (InterruptedException | DatabaseException | BrokenBarrierException | IOException e) {
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
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_STRING, MorphStreamEnv.get().configuration().getInt("spoutNum"));
        } else if (env.configuration().getInt("inputSourceType", 0) == 1) { //read input as JSON
            String inputFile = env.configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
            } else {
                String fileName = env.fileDataGenerator().prepareInputData(false);
                env.configuration().put("inputFilePath", fileName);
            }
            env.inputSource().initialize(env.configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_JSON, MorphStreamEnv.get().configuration().getInt("spoutNum"));
        }
    }
    public void start() throws InterruptedException {
        MeasureTools.Initialize();
        runTopologyLocally();
        //TODO: run for distributed mode
    }
    public void setSpoutCombo(String id, HashMap<String, TxnDescription> txnDescriptionHashMap, int numTasks) throws Exception {
        ApplicationSpoutCombo spout = new ApplicationSpoutCombo(id, txnDescriptionHashMap);
        env.setSpout(id, spout, numTasks);
    }
    public void setSpout(String id, int numTasks) throws Exception {
        ApplicationSpout spout = new ApplicationSpout(id);
        env.setSpout(id, spout, numTasks);
    }
    public AbstractBolt setBolt(String id, HashMap<String, TxnDescription> txnDescriptionHashMap, int numTasks, int fid, Grouping ... groups) {
        AbstractBolt bolt = null;
        switch (env.configuration().getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBolt(id, txnDescriptionHashMap, fid);
                break;
            }
            case CCOption_SStore:{
                bolt = new SStoreBolt(id, txnDescriptionHashMap, fid);
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
