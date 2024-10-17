package cli;

import intellistream.morphstream.api.Client;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class StateManagerRunner extends Client {
    private static final Logger log = LoggerFactory.getLogger(StateManagerRunner.class);
    private static ScheduledExecutorService memoryFootprintExecutor = Executors.newScheduledThreadPool(1);
    private static List<Double> memoryFootprint = new ArrayList<>();
    private static List<Long> gcFootprint = new ArrayList<>();
    private static List<Long> footprintTimestamps = new ArrayList<>();
    private static long managerStartTime = -1;

    /**
     * saData contains:
     * 0: saID
     * 1: txnAbortFlag
     * 2: saResult
     * 3 onwards: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */
    public static void main(String[] args) throws Exception {
        CliFrontend vnfMainClient = new CliFrontend("VNF_Main");
        vnfMainClient.loadConfigStreaming(args); // Load configuration, initialize DB

        boolean enableMemoryFootprint = (MorphStreamEnv.get().configuration().getInt("enableMemoryFootprint") == 1);
        if (enableMemoryFootprint) {
            startMemoryMonitoring();
        }

        vnfMainClient.initializeDB();
        vnfMainClient.createTransNFVStateManager(); // Create the global state manager object
        vnfMainClient.prepareTransNFVStateManager(); // Prepare state manager and state manager executors

        int numTPGThreads = MorphStreamEnv.get().configuration().getInt("tthread");
        vnfMainClient.registerOperator("sim_vnf", numTPGThreads);
        vnfMainClient.startTransNFVStateManager(); // Start CC threads, and wait for them to finish

    }

    public static void startMemoryMonitoring() {
        int memoryIntervalMS = MorphStreamEnv.get().configuration().getInt("memoryIntervalMS");
        memoryFootprintExecutor.scheduleAtFixedRate(new MemoryMonitorTask(), 0, memoryIntervalMS, TimeUnit.MILLISECONDS); // By default, every 10 milliseconds (10^-2)
    }

    public static void stopMemoryMonitoring() {
        memoryFootprintExecutor.shutdownNow();
        writeFootprintToCsv();
//        writeStartTimeCSV();
    }

    static class MemoryMonitorTask implements Runnable {
        private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        @Override
        public void run() {
            if (MorphStreamEnv.get().vnfExecutionFinished) {
                stopMemoryMonitoring();
                return;
            }
            // Memory Usage
            MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

            long heapUsed = heapMemoryUsage.getUsed();
            long nonHeapUsed = nonHeapMemoryUsage.getUsed();
            long totalUsed = heapUsed + nonHeapUsed;
            double totalUsedGB = totalUsed / 1073741824.0;

            // Garbage Collection
            long totalGcCount = 0;
            long totalGcTime = 0;
            for (GarbageCollectorMXBean gcBean : gcBeans) {
                totalGcCount += gcBean.getCollectionCount();
                totalGcTime += gcBean.getCollectionTime();
            }
            if (memoryFootprint.size() == 0) {
                managerStartTime = System.nanoTime();
            }
            memoryFootprint.add(totalUsedGB);
            gcFootprint.add(totalGcTime);
            footprintTimestamps.add(System.nanoTime());
        }
    }

    private static void writeFootprintToCsv() {
        String outputDir = VNFManager.getOutputFileDirectory();
        String filePath = String.format("%s/footprint.csv", outputDir);
        System.out.println("Writing to " + filePath);
        File dir = new File(outputDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Failed to create the directory.");
                return;
            }
        }
        File file = new File(filePath);
        if (file.exists()) {
            boolean isDeleted = file.delete();
            if (!isDeleted) {
                System.out.println("Failed to delete existing file.");
                return;
            }
        }
        try (FileWriter fw = new FileWriter(filePath, true);
             PrintWriter pw = new PrintWriter(fw)) {
            for (int i=0; i<memoryFootprint.size(); i++) {
                pw.println(footprintTimestamps.get(i) - managerStartTime + "," + memoryFootprint.get(i) + "," + gcFootprint.get(i));
            }
        } catch (IOException e) {
            throw new RuntimeException("An error occurred while writing to the CSV file.", e);
        }
    }

    private static void writeStartTimeCSV() {
        String experimentID = MorphStreamEnv.get().configuration().getString("experimentID");
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvExperimentPath");
        String ccStrategy = MorphStreamEnv.get().configuration().getString("ccStrategy");
        String baseDirectory = String.format("%s/%s/%s/%s", rootPath, "results", experimentID, "start_times");
        String filePath = String.format("%s/%s.csv", baseDirectory, ccStrategy);
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
        try (FileWriter fw = new FileWriter(filePath, true);
             PrintWriter pw = new PrintWriter(fw)) {
            pw.println(managerStartTime);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

