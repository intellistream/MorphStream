package java.intellistream.chc.database.manage;

import lombok.Getter;

import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.common.dao.Strategy;
import java.intellistream.chc.database.Configuration;
import java.intellistream.chc.database.manage.handler.impl.CrossflowHandler;
import java.intellistream.chc.database.manage.handler.impl.PerflowHandler;
import java.intellistream.chc.database.store.Database;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Database manager that manages the database states and handles the incoming access requests
 */
public class DBManager {
    public final ExecutorService executorService;
    private static DBManager manager;   // Singleton DBManager
    @Getter
    private final Database database;

    private DBManager() {
        this.executorService = Executors.newFixedThreadPool(Configuration.MANAGER_PARALLELISM); // initialize the thread pool
        this.database = new Database(); // initialize the database
    }

    public static DBManager getInstance() {
        if (DBManager.manager == null) {
            DBManager.manager = new DBManager();
        }
        return DBManager.manager;
    }

    /**
     * Submit the packet to the database manager
     * @param request the packet to be submitted
     */
    public void submit(Request request) {
        switch (Strategy.getStrategy(request.getPattern())) {
            case PER_FLOW:
                this.executorService.submit(new PerflowHandler(request));
                break;
            default:
                // i.e. CROSS_FLOW
                this.executorService.submit(new CrossflowHandler(request));
        }
    }
}
