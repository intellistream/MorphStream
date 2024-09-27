package client;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class MorphStreamClient {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamClient.class);
    private int clientNum;
    private String clientName;
    private List<Client> clients = new ArrayList<>();
    public MorphStreamClient() {
        this.clientNum =  MorphStreamEnv.get().configuration().getInt("clientNum");
        this.clientName = MorphStreamEnv.get().configuration().getString("clientClassName");
    }
    public void initialize() throws IOException {
        MorphStreamEnv.get().InputSourceInitialize();
        for (int i = 0; i < clientNum; i ++) {
            try {
                Class<?> clazz = Class.forName(clientName);
                Object instance = clazz.getDeclaredConstructor().newInstance();
                Client t = (Client) instance;
                t.initialize(i, MorphStreamEnv.get().clientLatch());
                clients.add(t);
                t.start();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                     InvocationTargetException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void MorphStreamClientJoin() throws InterruptedException {
        this.clients.get(0).join();
        boolean allFinished = false;
        while (!allFinished) {
            allFinished = true;
            for (Client client : clients) {
                if (client.isRunning) {
                    allFinished = false;
                    break;
                }
            }
            Thread.sleep(1000);
        }
        System.exit(0);
    }
}
