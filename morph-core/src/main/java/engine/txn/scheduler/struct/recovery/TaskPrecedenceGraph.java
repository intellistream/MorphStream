package engine.txn.scheduler.struct.recovery;

import engine.txn.durability.logging.LoggingEntry.PathRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.scheduler.context.recovery.RSContext;
import util.ConcurrentHashMap;

import java.util.ArrayDeque;
import java.util.Deque;


public class TaskPrecedenceGraph <Context extends RSContext>{
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final int app;
    public int isLogging;
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    public final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;//Exactly which OCs are executed by each thread.
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord;// Used by fault tolerance

    public ConcurrentHashMap<Integer, Task> idToTask;//Exactly which OCs are in one task.
    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS, int app) {
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        this.app = app;
        this.threadToContextMap = new ConcurrentHashMap<>();
        this.operationChains = new ConcurrentHashMap<>();
        this.threadToOCs = new ConcurrentHashMap<>();
    }
    public void initTPG(int offset) {
        if (app == 0) {//GS
            operationChains.put("MicroTable", new TableOCs(totalThreads,offset));
        } else if (app == 1) {//SL
            operationChains.put("accounts", new TableOCs(totalThreads,offset));
            operationChains.put("bookEntries", new TableOCs(totalThreads,offset));
        } else if (app == 2){//TP
            operationChains.put("segment_speed",new TableOCs(totalThreads,offset));
            operationChains.put("segment_cnt",new TableOCs(totalThreads,offset));
        } else if (app == 3) {//OB
            operationChains.put("goods",new TableOCs(totalThreads,offset));
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
            if (app == 0) {
                OperationChain gsOC = context.createTask("MicroTable", _key);
                operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            } else if (app == 1) {
                OperationChain accOC = context.createTask("accounts", _key);
                OperationChain beOC = context.createTask("bookEntries", _key);
                operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, accOC);
                operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, beOC);
                ocs.add(accOC);
                ocs.add(beOC);
            } else if (app == 2) {
                OperationChain speedOC = context.createTask("segment_speed",_key);
                OperationChain cntOC = context.createTask("segment_cnt",_key);
                operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, speedOC);
                operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, cntOC);
                ocs.add(speedOC);
                ocs.add(cntOC);
            } else if (app == 3) {
                OperationChain gsOC = context.createTask("goods", _key);
                operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            }
        }
        threadToOCs.put(context.thisThreadId, ocs);//Init task placing
    }
    private void resetOCs(Context context){
        if (app == 0) {
            operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 1) {
            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if( app == 2) {
            operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 3){
            operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
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


    public int getApp() {
        return app;
    }
}
