package benchmark.rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.rdma.MorphStreamDatabase;
import worker.rdma.MorphStreamDriver;
import worker.rdma.MorphStreamWorker;

import java.nio.ByteOrder;

public class RDMABenchmark {
    public static void main(String[] args) {
        try {
            MorphStreamEnv.get().LoadConfiguration(null, args); //TODO: add loadConfig from file
            if (MorphStreamEnv.get().isDriver()) {
                MorphStreamDriver driver = new MorphStreamDriver();
                driver.initialize();
                driver.start();
                driver.startClient();
                driver.MorphStreamDriverJoin();
            }  else if (MorphStreamEnv.get().isDatabase()) {
                MorphStreamDatabase morphStreamDatabase = new MorphStreamDatabase();
                morphStreamDatabase.start();
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
