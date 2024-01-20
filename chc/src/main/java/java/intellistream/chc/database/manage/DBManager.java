package java.intellistream.chc.database.manage;

import lombok.Getter;

import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.common.dao.Strategy;
import java.intellistream.chc.database.Configuration;
import java.intellistream.chc.database.manage.handle.impl.CrossflowHandle;
import java.intellistream.chc.database.manage.handle.impl.PerflowHandle;
import java.intellistream.chc.database.store.Database;

/**
 * Database manager that manages the database states and handles the incoming access requests
 */
public class DBManager {
    private final ThreadPoolService threadPoolService;
    private static DBManager manager;   // Singleton DBManager
    @Getter
    private final Database database;

    private DBManager() {
//        this.executorService = Executors.newFixedThreadPool(Configuration.MANAGER_PARALLELISM); // initialize the thread pool
        this.threadPoolService = new ThreadPoolService(Configuration.MANAGER_PARALLELISM);
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
     *
     * @param request the packet to be submitted
     */
    public void submit(Request request) {
        long toThreadId;
        switch (Strategy.getStrategy(request.getPattern())) {
            case PER_FLOW:
                if (database.containsExclusiveState(request.getVertexId(), request.getInstanceId(), request.getObjKey())) {
                    // i.e. the state is in the database
                    toThreadId = this.database.getExecuteThreadId(request.getVertexId(), request.getInstanceId(), request.getObjKey());
                    this.threadPoolService.submit(new PerflowHandle(request), toThreadId);
                } else {
                    // i.e. the state is not in the database, randomly choose a thread to execute the request
                    toThreadId = this.threadPoolService.pickSingleThread();
                    this.threadPoolService.submit(new PerflowHandle(request), toThreadId);
                    break;
                }
                break;
            default:
                // i.e. CROSS_FLOW
                if (database.containsSharedState(request.getVertexId(), request.getObjKey())) {
                    // i.e. the state is in the database
                    toThreadId = this.database.getExecuteThreadId(request.getVertexId(), request.getObjKey());
                    this.threadPoolService.submit(new CrossflowHandle(request), toThreadId);
                } else {
                    // i.e. the state is not in the database, randomly choose a thread to execute the request
                    toThreadId = this.threadPoolService.pickSingleThread();
                    this.threadPoolService.submit(new CrossflowHandle(request), toThreadId);
                }
        }
    }
}
