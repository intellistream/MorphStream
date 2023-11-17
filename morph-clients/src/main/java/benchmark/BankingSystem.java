package benchmark;

import client.BankingSystemClient;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.MorphStreamDriver;
import worker.MorphStreamWorker;

public class BankingSystem {
    public static void main(String[] args) {
        try {
            MorphStreamEnv.get().LoadConfiguration(null, args); //TODO: add loadConfig from file
            if (MorphStreamEnv.get().isDriver()) {
                MorphStreamDriver driver = new MorphStreamDriver();
                driver.initialize();
                driver.start();
                BankingSystemClient.startClient(new String[]{});
            } else {
                MorphStreamWorker morphStreamWorker = new MorphStreamWorker();
                morphStreamWorker.prepare();
                BankingSystemClient client = new BankingSystemClient();
                morphStreamWorker.registerFunction(client.txnDescriptions);
                morphStreamWorker.prepare();
                morphStreamWorker.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
