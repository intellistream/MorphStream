package cli;

import intellistream.morphstream.api.Client;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import message.VNFCtrlServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.CountDownLatch;


public class StateManagerRunner extends Client {
    private static final Logger log = LoggerFactory.getLogger(StateManagerRunner.class);

    /**
     * saData contains:
     * 0: saID
     * 1: txnAbortFlag
     * 2: saResult
     * 3 onwards: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */
    public static void main(String[] args) throws Exception {

        CliFrontend vnfMain = new CliFrontend("VNF_Main");
        vnfMain.loadConfigStreaming(args); // Load configuration, initialize DB
        vnfMain.prepareAdaptiveCC(); // Create AdaptiveCCManager, which initializes TPG queues

        int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
        if (communicationChoice == 0) { // Java VNF instances
            int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
            vnfMain.registerOperator("sim_vnf", numTPGThreads);
            vnfMain.start(); //This will continuously run until TPG threads receive stop signals

        } else if (communicationChoice == 1) {

            // Build connection with LibVNF VNF instances
            VNFCtrlServer vnfCtrlServer = new VNFCtrlServer();
            int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
            vnfCtrlServer.listenForInstances(8080, vnfInstanceNum);

            // Wait for VNF instances to send JSON
            while (MorphStreamEnv.get().vnfJSON == null) {
                Thread.sleep(1000);
            }
            String cleanedJson = cleanupJson(MorphStreamEnv.get().vnfJSON);
            System.out.println(cleanedJson);
            VNFJsonClass vnfJsonClass;

            ObjectMapper mapper = new ObjectMapper();
            vnfJsonClass = mapper.readValue(cleanedJson, VNFJsonClass.class);

            // Manually assign txnID and saID
            for (App app : vnfJsonClass.getApps()) {
                int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
                vnfMain.registerOperator(app.getName(), numTPGThreads);

                int txnIndex = 0;
                for (Transaction txn : app.getTransactions()) {
                    txn.setTxnID(txnIndex);
                    int saIndex = 0;
                    for (StateAccess sa : txn.getStateAccesses()) {
                        String saType = "read-write"; // TODO: Hardcoded as read-write, need JSON to specify
                        vnfMain.registerStateAccess(String.valueOf(saIndex), saType, sa.getTableName());

                        sa.setSaID(saIndex);
                        switch (saType) {
                            case "read":
                                MorphStreamEnv.get().updateSATypeMap(saIndex, 0);
                                break;
                            case "write":
                                MorphStreamEnv.get().updateSATypeMap(saIndex, 1);
                                break;
                            case "read-write":
                                MorphStreamEnv.get().updateSATypeMap(saIndex, 2);
                                break;
                        }
                        MorphStreamEnv.get().updateSATableNameMap(saIndex, sa.getTableName());
                        saIndex++;
                    }
                    txnIndex++;
                }
            }
            System.out.println("Deserialized SFC Json data: " + vnfJsonClass.getApps().get(0).getName());
            vnfMain.start(); // Start TPG_CC threads, at this stage all manager threads are ready to process requests
        }
    }


    public static String cleanupJson(String messyJson) {
        // Step 1: Remove all newline characters and excessive spaces.
        String cleaned = messyJson.replaceAll("\\s+", " ");

        // Step 2: Attempt to concatenate broken strings correctly.
        cleaned = cleaned.replace(" , ", ",");
        cleaned = cleaned.replace(", ", ",");
        cleaned = cleaned.replace(" ,", ",");

        // Step 3: Handle misplaced quotation marks and commas.
        cleaned = cleaned.replace("\" ,\"", "\",\"");
        cleaned = cleaned.replace("\" , \"", "\",\"");

        // Step 4: Remove leading and trailing spaces for all array and object brackets.
        cleaned = cleaned.replace("[ ", "[");
        cleaned = cleaned.replace(" ]", "]");
        cleaned = cleaned.replace("{ ", "{");
        cleaned = cleaned.replace(" }", "}");

        return cleaned;
    }
}
