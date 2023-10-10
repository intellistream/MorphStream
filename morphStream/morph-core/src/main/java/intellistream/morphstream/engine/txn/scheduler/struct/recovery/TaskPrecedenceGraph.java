package intellistream.morphstream.engine.txn.scheduler.struct.recovery;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.PathRecord;
import intellistream.morphstream.engine.txn.scheduler.context.recovery.RSContext;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;


public class TaskPrecedenceGraph<Context extends RSContext> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final int totalThreads;
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    public final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;//Exactly which OCs are executed by each thread.
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    public int isLogging;
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord;// Used by fault tolerance

    public ConcurrentHashMap<Integer, Task> idToTask;//Exactly which OCs are in one task.
    private final String[] tableNames;

    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS) {
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        this.threadToContextMap = new ConcurrentHashMap<>();
        this.operationChains = new ConcurrentHashMap<>();
        this.threadToOCs = new ConcurrentHashMap<>();
        tableNames = MorphStreamEnv.get().configuration().getString("tableNames").split(",");
    }

    public void initTPG(int offset) {
        for (String tableName : tableNames) {
            operationChains.put(tableName, new TableOCs(totalThreads, offset));
        }
    }

    public void setOCs(Context context) {
        ArrayDeque<OperationChain> ocs = new ArrayDeque<>();
        int left_bound = context.thisThreadId * delta;
        int right_bound;
        if (context.thisThreadId == totalThreads - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (context.thisThreadId + 1) * delta;
        }
        resetOCs(context);
        String _key;
        for (int key = left_bound; key < right_bound; key++) {
            _key = String.valueOf(key);
            for (String tableName : tableNames) {
                OperationChain oc = context.createTask(tableName, _key);
                operationChains.get(tableName).threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, oc);
                ocs.add(oc);
            }
        }
        threadToOCs.put(context.thisThreadId, ocs);//Init task placing
    }

    private void resetOCs(Context context) {
        for (String tableName : tableNames) {
            operationChains.get(tableName).threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        }
    }

    public TableOCs getTableOCs(String tableName) {
        return operationChains.get(tableName);
    }

    public ConcurrentHashMap<String, TableOCs> getOperationChains() {
        return operationChains;
    }

    public OperationChain getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
    }

    private OperationChain getOC(String tableName, String pKey, int threadId) {
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
    }

    public OperationChain addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.pKey;
        OperationChain retOc = getOC(table_name, primaryKey, operation.context.thisThreadId);
        retOc.addOperation(operation);
        return retOc;
    }
}
