package java.intellistream.chc.database.manage;

import java.intellistream.chc.common.dao.Packet;
import java.intellistream.chc.common.dao.Strategy;
import java.intellistream.chc.database.Configuration;
import java.intellistream.chc.database.manage.handler.impl.CrossflowHandler;
import java.intellistream.chc.database.manage.handler.impl.PerflowHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Database manager that manages the database states and handles the incoming access requests
 */
public class DBManager {
    public final ExecutorService executorService;
    private static DBManager manager;   // Singleton DBManager

    private DBManager() {
        this.executorService = Executors.newFixedThreadPool(Configuration.MANAGER_PARALLELISM); // initialize the thread pool
    }

    public static DBManager getInstance() {
        if (DBManager.manager == null) {
            DBManager.manager = new DBManager();
        }
        return DBManager.manager;
    }

    /**
     * Submit the packet to the database manager
     * @param packet the packet to be submitted
     */
    public void submit(Packet packet) {
        switch (Strategy.getStrategy(packet.getPattern())) {
            case PER_FLOW:
                this.executorService.submit(new PerflowHandler(packet));
                break;
            default:
                // i.e. CROSS_FLOW
                this.executorService.submit(new CrossflowHandler(packet));
        }
    }
}
