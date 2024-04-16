package cli;

import intellistream.morphstream.api.Client;
// import intellistream.morphstream.common.io.ByteIO.InputWithDecompression.NativeDataInputView;

import intellistream.morphstream.api.input.AdaptiveCCManager;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.HashMap;

import static cli.CliFrontend.*;

public class FastSLClient extends Client {
    private static final Logger log = LoggerFactory.getLogger(FastSLClient.class);

    /**
     * saData contains:
     * 0: saID
     * 1: txnAbortFlag
     * 2: saResult
     * 3 onwards: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */

    public static void main(String[] args) throws Exception {
        CliFrontend fastSLClient = new CliFrontend("FastSLClient");
        fastSLClient.loadConfigStreaming(args);

        NativeInterface VNF_JNI = new NativeInterface();
        String[] param = {""};
        String sfcJSON = VNF_JNI.__init_SFC(1, param);
        String cleanedJson = cleanupJson(sfcJSON);
        System.out.println(cleanedJson);
        VNFJsonClass vnfJsonClass = null;

        try {
            ObjectMapper mapper = new ObjectMapper();
            vnfJsonClass = mapper.readValue(cleanedJson, VNFJsonClass.class);
            System.out.println("Deserialized data: " + vnfJsonClass.getApps().get(0).getName());
        } catch (Exception e) {
            e.printStackTrace();
        }

        //TODO: Integrate the deserialized JSON data with the rest of the system

        fastSLClient.registerStateObject("srcAccountBalance", "accounts", 0, 1, "WRITE");
        fastSLClient.registerStateObject("destAccountBalance", "accounts", 1, 1, "WRITE");
        String[] srcTransferStateObjs = {"srcAccountBalance"};
        String[] destTransferStateObjs = {"srcAccountBalance", "destAccountBalance"};

        fastSLClient.registerStateAccess("0", srcTransferStateObjs, null, "WRITE"); //deposit
        fastSLClient.registerStateAccess("1", srcTransferStateObjs, null, "WRITE"); //srcTransfer
        fastSLClient.registerStateAccess("2", destTransferStateObjs, null, "WRITE"); //destTransfer
        String[] transferStateAccessIDs = {"1", "2"};
        String[] depositStateAccessIDs = {"0"};

        fastSLClient.registerTxn("0", depositStateAccessIDs);
        fastSLClient.registerTxn("1", transferStateAccessIDs);
        String[] txnIDs = {"transfer", "deposit"};

        fastSLClient.registerOperator("fastSLClient", txnIDs, 0, 4);

        // Start all 4 CC strategies
        AdaptiveCCManager adaptiveCCManager = new AdaptiveCCManager();
        adaptiveCCManager.initialize();

        for (App app : vnfJsonClass.getApps()) {
            for (Transaction txn : app.getTransactions()) {
                for (StateAccess sa : txn.getStateAccesses()) {
                    switch (sa.getType()) {
                        case "read":
                            adaptiveCCManager.updateSATypeMap(Integer.parseInt(sa.getTableName()), 0);
                            break;
                        case "write":
                            adaptiveCCManager.updateSATypeMap(Integer.parseInt(sa.getTableName()), 1);
                            break;
                        case "read-write":
                            adaptiveCCManager.updateSATypeMap(Integer.parseInt(sa.getTableName()), 2);
                            break;
                    }
                }
            }
        }


        fastSLClient.start();
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
