package cli;

import intellistream.morphstream.api.Client;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import message.VNFCtrlServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


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
        prepareFilePath();
        boolean enableMemoryFootprint = (MorphStreamEnv.get().configuration().getInt("enableMemoryFootprint") == 1);
        if (enableMemoryFootprint) {
            startMemoryMonitoring();
        }

        CliFrontend vnfMain = new CliFrontend("VNF_Main");
        vnfMain.loadConfigStreaming(args); // Load configuration, initialize DB
        vnfMain.prepareAdaptiveCC(); // Create AdaptiveCCManager, which initializes TPG queues

        int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
        if (communicationChoice == 0) { // Java VNF instances
            int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
            vnfMain.registerOperator("sim_vnf", numTPGThreads);
            vnfMain.start();

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

    public static void startMemoryMonitoring() {
        int memoryIntervalMS = MorphStreamEnv.get().configuration().getInt("memoryIntervalMS");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new MemoryMonitorTask(), 0, memoryIntervalMS, TimeUnit.MILLISECONDS);
    }

    static class MemoryMonitorTask implements Runnable {
        private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        private static final String CSV_FILE_PATH = "memory_usage.csv";

        @Override
        public void run() {
            // Memory Usage
            MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

            long heapUsed = heapMemoryUsage.getUsed();
            long nonHeapUsed = nonHeapMemoryUsage.getUsed();
            long totalUsed = heapUsed + nonHeapUsed;

            // Garbage Collection
            long totalGcCount = 0;
            long totalGcTime = 0;
            for (GarbageCollectorMXBean gcBean : gcBeans) {
                totalGcCount += gcBean.getCollectionCount();
                totalGcTime += gcBean.getCollectionTime();
            }

            // Write to CSV
            writeToCsv(totalUsed, totalGcTime);

            // Output
            System.out.println("Heap Memory: Used = " + heapUsed + " bytes");
            System.out.println("Non-Heap Memory: Used = " + nonHeapUsed + " bytes");
            System.out.println("Total Memory: Used = " + totalUsed + " bytes");
            System.out.println("Total GC Count: " + totalGcCount);
            System.out.println("Total GC Time: " + totalGcTime + " ms");

        }

        private void writeToCsv(long totalMemoryUsed, long totalGcTime) {
            try (FileWriter fw = new FileWriter(CSV_FILE_PATH, true);
                 PrintWriter pw = new PrintWriter(fw)) {
                pw.println(totalMemoryUsed + "," + totalGcTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void prepareFilePath() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");
        int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "memory_footprint");
        String filePath = String.format("%s/%s.csv", baseDirectory, toStringStrategy(ccStrategy));
        System.out.println("Writing to " + filePath);
        File dir = new File(baseDirectory);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create the directory.");
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                throw new RuntimeException("Failed to delete existing file.");
            }
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

    private static String toStringStrategy(int ccStrategy) {
        if (ccStrategy == 0) {
            return "Partitioning";
        } else if (ccStrategy == 1) {
            return "Replication";
        } else if (ccStrategy == 2) {
            return "Offloading";
        } else if (ccStrategy == 3) {
            return "Preemptive";
        } else if (ccStrategy == 4) {
            return "OpenNF";
        } else if (ccStrategy == 5) {
            return "CHC";
        } else if (ccStrategy == 6) {
            return "S6";
        } else if (ccStrategy == 7) {
            return "TransNFV";
        } else {
            return "Invalid";
        }
    }
}
