package cli;

import intellistream.morphstream.api.Client;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;
import message.VNFCtrlServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;


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

        CliFrontend vnfClient = new CliFrontend("FastSLClient");
        vnfClient.loadConfigStreaming(args);
        vnfClient.prepareAdaptiveCC(); // Create AdaptiveCCManager, which initializes TPG queues

        boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);
        if (serveRemoteVNF) {

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
                vnfClient.registerOperator(app.getName(), numTPGThreads);

                int txnIndex = 0;
                for (Transaction txn : app.getTransactions()) {
                    txn.setTxnID(txnIndex++);
                    int saIndex = 0;
                    for (StateAccess sa : txn.getStateAccesses()) {
                        vnfClient.registerStateAccess(String.valueOf(saIndex), sa.getType(), sa.getTableName());

                        sa.setSaID(saIndex++);
                        switch (sa.getType()) {
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
                    }
                }
            }
            System.out.println("Deserialized SFC Json data: " + vnfJsonClass.getApps().get(0).getName());

            vnfClient.startAdaptiveCC(); // Start Partition_CC, Cache_CC, Offload_CC, and Monitor threads
            vnfClient.start(); // Start TPG_CC threads, at this stage all manager threads are ready to process requests

        } else {
            vnfClient.startAdaptiveCC();
            // A hardcoded overall-performance measurement latch
            MorphStreamEnv.get().simVNFLatch = new CountDownLatch(MorphStreamEnv.get().configuration().getInt("vnfInstanceNum"));
            Thread simVNFCompletionMonitor = new Thread(() -> {
                try {
                    MorphStreamEnv.get().simVNFLatch.await();
                    double overallThroughput = MorphStreamEnv.get().adaptiveCCManager().joinVNFInstances();
                    System.out.println("All VNF instances have completed processing.");
                    System.out.println("Overall throughput: " + overallThroughput + " events/second");
                    writeToCSV(MorphStreamEnv.get().adaptiveCCManager().getPattern(), MorphStreamEnv.get().adaptiveCCManager().getCCStrategy(), overallThroughput);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            simVNFCompletionMonitor.start();

            int tpgThreads = MorphStreamEnv.get().configuration().getInt("tthread");
            vnfClient.registerOperator("sim_vnf", tpgThreads);
            vnfClient.start(); //This will continuously run until TPG threads receive stop signals
        }
    }


    public static void writeToCSV(String pattern, String ccStrategy, double throughput) {
        // Path where the directory and file will be created
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        String baseDirectory = rootPath + "/experiments/pre_study";
        String directoryPath = String.format("%s/%s", baseDirectory, pattern);
        String filePath = String.format("%s/%s.csv", directoryPath, ccStrategy);
        System.out.println("Writing to " + filePath);

        // Ensure directory exists
        File dir = new File(directoryPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return; // Stop further processing if unable to create the directory
            }
        }

        // Create a File object to represent the path
        File file = new File(filePath);

        // Check if the file exists, and delete it if it does
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return; // Stop further processing if unable to delete the file
            }
        }

        // Using try-with-resources to handle file closing
        try (FileWriter fileWriter = new FileWriter(file)) {
            // Create the line of data to write
            String lineToWrite = pattern + "," + ccStrategy + "," + throughput + "\n";

            // Write the line to the file
            fileWriter.write(lineToWrite);

            // Feedback to know operation was successful
            System.out.println("Data written to CSV file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the CSV file.");
            e.printStackTrace();
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
