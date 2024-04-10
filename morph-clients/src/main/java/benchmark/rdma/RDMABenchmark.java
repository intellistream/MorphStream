package benchmark.rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.rdma.MorphStreamDriver;
import worker.rdma.MorphStreamWorker;

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
