package intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats;

import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class OdpStats {
    private String[] statFsOdpFiles = {
            "invalidations_faults_contentions", "num_invalidation_pages",
            "num_invalidations", "num_page_fault_pages", "num_page_faults",
            "num_prefetches_handled", "num_prefetch_pages"
    };
    private String sysFsFolder;

    private long[] initialOdpStat;
    public OdpStats(RdmaShuffleConf rdmaShuffleConf) {
        sysFsFolder = "/sys/class/infiniband_verbs/uverbs" + rdmaShuffleConf.rdmaDeviceNum;
        initialOdpStat = new long[statFsOdpFiles.length];
        for (int i = 0; i < statFsOdpFiles.length; i++) {
            initialOdpStat[i] = readLongFromFile(sysFsFolder + "/" + statFsOdpFiles[i]);
        }
    }
    public void printODPStatistics() {
        long[] odpStats = new long[statFsOdpFiles.length];
        for (int i = 0; i < statFsOdpFiles.length; i++) {
            odpStats[i] = readLongFromFile(sysFsFolder + "/" + statFsOdpFiles[i]);
        }

        long[] diff = new long[statFsOdpFiles.length];
        for (int i = 0; i < statFsOdpFiles.length; i++) {
            diff[i] = odpStats[i] - initialOdpStat[i];
        }

        for (int i = 0; i < statFsOdpFiles.length; i++) {
            System.out.println(statFsOdpFiles[i] + ": " + diff[i]);
        }
    }
    private static long readLongFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)))) {
            return Long.parseLong(reader.readLine().trim());
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
