package cli;

import intellistream.morphstream.api.Client;

import intellistream.morphstream.api.input.AdaptiveCCManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;


public class FastSLClient extends Client {
    private static final Logger log = LoggerFactory.getLogger(FastSLClient.class);
    private static final boolean serveRemoteVNF = MorphStreamEnv.get().configuration().getBoolean("serveRemoteVNF");

    /**
     * saData contains:
     * 0: saID
     * 1: txnAbortFlag
     * 2: saResult
     * 3 onwards: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */
    public static void main(String[] args) throws Exception {

        CliFrontend vnfClient = new CliFrontend("FastSLClient");
        vnfClient.loadConfigStreaming(args);

        if (serveRemoteVNF) {
            NativeInterface VNF_JNI = new NativeInterface();
            String[] param = {""};
            String sfcJSON = VNF_JNI.__init_SFC(1, param);
            String cleanedJson = cleanupJson(sfcJSON);
            System.out.println(cleanedJson);
            VNFJsonClass vnfJsonClass;

            // Start all 4 CC strategies
            AdaptiveCCManager adaptiveCCManager = MorphStreamEnv.get().adaptiveCCManager();

            try {
                ObjectMapper mapper = new ObjectMapper();
                vnfJsonClass = mapper.readValue(cleanedJson, VNFJsonClass.class);

                // Manually assign txnID and saID
                for (App app : vnfJsonClass.getApps()) {
                    int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
                    vnfClient.registerOperator(app.getName(), vnfInstanceNum);

                    int txnIndex = 0;
                    for (Transaction txn : app.getTransactions()) {
                        txn.setTxnID(txnIndex++);
                        int saIndex = 0;
                        for (StateAccess sa : txn.getStateAccesses()) {
                            vnfClient.registerStateAccess(String.valueOf(saIndex), sa.getType(), sa.getTableName());

                            sa.setSaID(saIndex++);
                            switch (sa.getType()) {
                                case "read":
                                    adaptiveCCManager.updateSATypeMap(saIndex, 0);
                                    break;
                                case "write":
                                    adaptiveCCManager.updateSATypeMap(saIndex, 1);
                                    break;
                                case "read-write":
                                    adaptiveCCManager.updateSATypeMap(saIndex, 2);
                                    break;
                            }
                            adaptiveCCManager.updateSATableNameMap(saIndex, sa.getTableName());
                        }
                    }
                }
                System.out.println("Deserialized data: " + vnfJsonClass.getApps().get(0).getName());

            } catch (Exception e) {
                e.printStackTrace();
            }

            vnfClient.start();

        } else {
            // Start all 4 CC strategies
            AdaptiveCCManager adaptiveCCManager = MorphStreamEnv.get().adaptiveCCManager();
            vnfClient.registerOperator("sim_vnf", 4);
            adaptiveCCManager.startSimVNF(); //TODO: Hardcoded with initial timeout of 5 seconds to wait for TPG threads to be ready
            vnfClient.start();
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
