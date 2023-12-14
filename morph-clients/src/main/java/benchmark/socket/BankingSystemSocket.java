package benchmark.socket;

import client.BankingSystemClient;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.socket.MorphStreamDriver;
import worker.socket.MorphStreamWorker;

public class BankingSystemSocket {
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
                BankingSystemClient client = new BankingSystemClient();
                client.defineFunction();
                morphStreamWorker.registerFunction(client.txnDescriptions);
                morphStreamWorker.start();
                morphStreamWorker.join(10000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
