package benchmark.rdma;

import client.BankingSystemClient;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.rdma.MorphStreamDriver;
import worker.rdma.MorphStreamWorker;

public class BankingSystemRdma {
    public static void main(String[] args) {
        try {
            MorphStreamEnv.get().LoadConfiguration(null, args); //TODO: add loadConfig from file
            if (MorphStreamEnv.get().isDriver()) {
                MorphStreamDriver driver = new MorphStreamDriver();
                driver.initialize();
                driver.start();
                BankingSystemClient.startClient(new String[]{});
                driver.MorphStreamDriverJoin();
            } else {
                MorphStreamWorker morphStreamWorker = new MorphStreamWorker();
                MorphStreamEnv.get().setRdmaWorkerManager(morphStreamWorker.getRdmaWorkerManager());
                BankingSystemClient client = new BankingSystemClient();
                client.defineFunction();
                morphStreamWorker.initialize(client.txnDescriptions);
                morphStreamWorker.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
