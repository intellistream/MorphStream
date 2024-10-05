package benchmark.rdma;

import client.MorphStreamClient;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import worker.rdma.MorphStreamDatabase;
import worker.rdma.MorphStreamDriver;
import worker.rdma.MorphStreamWorker;

import java.nio.ByteOrder;

public class RDMABenchmark {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(RDMABenchmark.class);
    public static void main(String[] args) {
        try {
            MorphStreamEnv.get().LoadConfiguration(null, args); //TODO: add loadConfig from file
            if (MorphStreamEnv.get().isDriver()) {
                MorphStreamDriver driver = new MorphStreamDriver();
                driver.initialize();
                driver.start();
                driver.MorphStreamDriverJoin();
            } else if (MorphStreamEnv.get().isClient()) {
                MorphStreamClient client = new MorphStreamClient();
                client.initialize();
                client.MorphStreamClientJoin();
            } else if (MorphStreamEnv.get().isDatabase()) {
                long start = System.currentTimeMillis();
                MorphStreamDatabase morphStreamDatabase = new MorphStreamDatabase();
                morphStreamDatabase.start();
                long end = System.currentTimeMillis();
                LOG.info("Database started in " + (end - start - 1) + " ms");
            } else {
                MorphStreamWorker morphStreamWorker = new MorphStreamWorker();
                morphStreamWorker.initialize();
                morphStreamWorker.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
