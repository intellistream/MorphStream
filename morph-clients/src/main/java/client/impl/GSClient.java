package client.impl;

import client.CliFrontend;
import client.Configuration;
import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.Result;
import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;

import static intellistream.morphstream.configuration.CONTROL.enable_log;


public class GSClient extends Client {
    private static final Logger log = LoggerFactory.getLogger(GSClient.class);

    /**
     * Client-defined customized txn-UDF, which will be executed in Schedulers
     * access.udfResult is the value to be written into schemaRecord
     *
     * @param access Stores everything bolt needs, including StateObjects updated by Scheduler
     * @return true if txn-UDF is executed successfully, false if txn-UDF is aborted
     */
    @Override
    public boolean transactionUDF(StateAccess access) {
        String txnName = access.getTxnName();
        if (Objects.equals(txnName, "gsTransaction")) {
            String stateAccessName = access.getStateAccessName();
            if (Objects.equals(stateAccessName, "gsStateAccess")) {
                double microState1 = access.getStateObject("microState1").getDoubleValue("value");
                double microState2 = access.getStateObject("microState2").getDoubleValue("value");
                double microState3 = access.getStateObject("microState3").getDoubleValue("value");
                double microState4 = access.getStateObject("microState4").getDoubleValue("value");
                double microState5 = access.getStateObject("microState5").getDoubleValue("value");
                access.udfResult = (microState1 + microState2 + microState3 + microState4 + microState5) / 5;
                return true;
            } else {
                return false; //abort txn
            }
        } else {
            return false;
        }
    }

    @Override
    public Result postUDF(String txnName, HashMap<String, StateAccess> stateAccessMap) {
        Result result = new Result();
        for (StateAccess stateAccess : stateAccessMap.values()) {
            if (stateAccess.isAborted()) {
                String[] abortResult = {"aborted"};
                result.setResults(abortResult);
                return result;
            }
        }
        if (Objects.equals(txnName, "gsTransaction")) {
            StateAccess gsStateAccess = stateAccessMap.get("gsStateAccess");
            Double[] stateAccessResults = new Double[1];

            try {
                stateAccessResults[0] = gsStateAccess.getStateObject("microState1").getDoubleValue("value");
            } catch (NullPointerException e) {
                stateAccessResults[0] = 0.0;
            }
            result.setResults(stateAccessResults);
        }
        return result;
    }
    
    public static void startJob(String[] args) throws Exception {
        CliFrontend gsClientJob = CliFrontend.getOrCreate().appName("GSClient");
//        SLClient.LoadConfiguration("/home/resources/SLClient.properties", args);
        gsClientJob.LoadConfiguration(null, args); //TODO: add loadConfig from file
        gsClientJob.prepare();

        //Initialize transactions for Combo to execute
        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>(); //Flag -> TxnDescription

        //Define transfer transaction
        TxnDescription transferDescriptor = new TxnDescription();
        //Define transfer's 1st state accesses
        StateAccessDescription gsStateAccess = new StateAccessDescription("gsStateAccess", AccessType.WRITE);
        gsStateAccess.addStateObjectDescription("microState1", AccessType.WRITE, "microTable", "key", "value", 0);
        gsStateAccess.addStateObjectDescription("microState2", AccessType.READ, "microTable", "key", "value", 1);
        gsStateAccess.addStateObjectDescription("microState3", AccessType.READ, "microTable", "key", "value", 2);
        gsStateAccess.addStateObjectDescription("microState4", AccessType.READ, "microTable", "key", "value", 3);
        gsStateAccess.addStateObjectDescription("microState5", AccessType.READ, "microTable", "key", "value", 4);
        gsStateAccess.addValueName("value");
        //Add state accesses to transaction
        transferDescriptor.addStateAccess("gsStateAccess", gsStateAccess);
        txnDescriptions.put("gsTransaction", transferDescriptor);

        //Define topology
        gsClientJob.setSpoutCombo("gs", txnDescriptions, MorphStreamEnv.get().configuration().getInt("tthread"));
        // create an array of operator IDs for monitoring
        RuntimeMonitor.setOperatorIDs(new String[]{"gs"});
        RuntimeMonitor.setDataPath(Configuration.JOB_INFO_PATH + "/-1");

        //Initiate runner
        try {
            gsClientJob.run();
        } catch (InterruptedException ex) {
            if (enable_log) log.error("Error in running topology locally", ex);
        }
    }

    public static void main(String[] args) throws Exception {
//        JobInitializeUtil.initialize("3");
        startJob(args);
    }
}
