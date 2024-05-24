package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class TaskPrecedenceGraph<Context extends DSContext> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    private final ConcurrentHashMap<String, Integer> tableNameToDelta = new ConcurrentHashMap<>();//range of each partition. depends on the number of thread in the stage.
    private final int totalThreads;
    private final ConcurrentHashMap<String, Integer> tableNameToNumItems = new ConcurrentHashMap<>();
    @Getter
    private final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;//Exactly which OCs are executed by each thread.
    @Getter
    private final ConcurrentHashMap<Integer, Context> threadToContext;//
    private final ConcurrentHashMap<String, TableOCs<OperationChain>> tableNameToOCs;//shared data structure.
    private final String[] tableNames;

    public TaskPrecedenceGraph(int totalThreads, int numItems) {
        this.totalThreads = totalThreads;
        this.threadToOCs = new ConcurrentHashMap<>();
        this.tableNameToOCs = new ConcurrentHashMap<>();
        this.threadToContext = new ConcurrentHashMap<>();
        tableNames = MorphStreamEnv.get().configuration().getString("tableNames").split(";");
        for (String tableName : tableNames) {
            tableNameToNumItems.put(tableName, MorphStreamEnv.get().configuration().getInt(tableName + "_num_items", 1000000));
            tableNameToDelta.put(tableName, tableNameToNumItems.get(tableName) / totalThreads);
        }
    }
    public void initTPG() {
        for (String tableName : tableNames) {
            tableNameToOCs.put(tableName, new TableOCs<OperationChain>(totalThreads));
        }
    }
    public void setOCs(Context context) {
        ArrayDeque<OperationChain> ocs = new ArrayDeque<>();
        resetOCs(context);
        String _key;
        for (String tableName : tableNames) {
            int left_bound = context.thisThreadId * tableNameToDelta.get(tableName);
            int right_bound;
            if (context.thisThreadId == totalThreads - 1) {//last executor need to handle left-over
                right_bound = tableNameToNumItems.get(tableName);
            } else {
                right_bound = (context.thisThreadId + 1) * tableNameToDelta.get(tableName);
            }
            for (int key = left_bound; key < right_bound; key++) {
                _key = String.valueOf(key);
                OperationChain oc = context.createTask(tableName, _key);
                tableNameToOCs.get(tableName).threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, oc);
                ocs.add(oc);
            }
        }

        threadToOCs.put(context.thisThreadId, ocs);//Init task placing
    }
    public void setupOperations(HashMap<String, Operation> operationHashMap) {
        for (Map.Entry<String, Operation> entry : operationHashMap.entrySet()) {
            String tableName = entry.getValue().table_name;
            //Father setup
            for (String stateAccessName : entry.getValue().function.getFatherStateAccessNames()) {
                entry.getValue().addToFather(operationHashMap.get(stateAccessName));
            }
            //Brother setup
            if (entry.getValue().txn_context.getTransactionCombo().get(entry.getKey()) != null) {
                for (String brotherName : entry.getValue().txn_context.getTransactionCombo().get(entry.getKey())) {
                    entry.getValue().addBrother(operationHashMap.get(brotherName));
                }
            }
            //Add to operation chain
            getOC(tableName, entry.getValue().pKey).addOperation(entry.getValue());
        }
    }
    public void setupRemoteOperations(List<Operation> operations) {
        for (Operation operation : operations) {
            String tableName = operation.table_name;
            getOC(tableName, operation.pKey).addOperation(operation);
        }
    }
    public void setupDependencies(Context context) {
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            if (!oc.operations.isEmpty()) {
                oc.updateDependencies();
            }
        }
    }
    private void resetOCs(Context context) {
        for (String tableName : tableNames) {
            tableNameToOCs.get(tableName).threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        }
    }
    public TableOCs<OperationChain> getTableOCs(String tableName) {
        return tableNameToOCs.get(tableName);
    }
    public OperationChain getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / tableNameToDelta.get(tableName);
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContext.get(threadId).createTask(tableName, pKey));
    }
}
