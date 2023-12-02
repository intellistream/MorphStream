package intellistream.morphstream.engine.txn.scheduler.struct.op;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.PathRecord;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSchedulerContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_no;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_path;

/**
 * TPG  -> Partition -> Key:OperationChain -> Operation-Operation-Operation...
 * |            |
 * |            -> Key: OperationChain -> Operation-Operation...
 * |
 * -> Partition ...
 */

/**
 * TPG  -> Key:OperationChain [ Operation-Operation-Operation...]
 * |
 * -> Key: OperationChain [ Operation-Operation... ]
 * |
 * -> Key: OperationChain [ Operation... ]
 */
public class TaskPrecedenceGraph<Context extends OPSchedulerContext> {
    private static final Logger log = LoggerFactory.getLogger(TaskPrecedenceGraph.class);

    public final int totalThreads;
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    public final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
//    private final int app;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    private final ConcurrentHashMap<String, Vector<Operation>> NonOperations = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord;// Used path logging
    public int isLogging = LOGOption_no;
    CyclicBarrier barrier;
    private int maxLevel = 0; // just for layered scheduling
    private final String[] tableNames;


    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS) {
        barrier = new CyclicBarrier(totalThreads);
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        // all parameters in this class should be thread safe.
        threadToContextMap = new ConcurrentHashMap<>();
        threadToOCs = new ConcurrentHashMap<>();
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        tableNames = MorphStreamEnv.get().configuration().getString("tableNames").split(",");
    }

    public void reset(Context context) {
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            oc.clear(); // only need to clear all operations from all ocs
        }
        for (Vector<Operation> ops : this.NonOperations.values()) {
            ops.clear();
        }
        if (context.thisThreadId == 0) log.info("===Clear current data for the next batch===");
    }

    public void initTPG(int offset) {
        for (String tableName : tableNames) {
            operationChains.put(tableName, new TableOCs(totalThreads, offset));
            NonOperations.put(tableName, new Vector<>());
        }
    }

    /**
     * Pre-create a bunch of OCs for each key in the table, which reduces the constant overhead during the runtime.
     *
     * @param context
     */
    // TODO: if running multiple batches, this will be a problem.
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
        threadToOCs.put(context.thisThreadId, ocs);
    }

    private void resetOCs(Context context) {
        for (String tableName : tableNames) {
            operationChains.get(tableName).threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        }
    }

    public TableOCs getTableOCs(String table_name) {
        return operationChains.get(table_name);
    }

    public boolean isThreadTOCsReady() {
        return threadToOCs.size() == totalThreads;
    }

    public ConcurrentHashMap<String, TableOCs> getOperationChains() {
        return operationChains;
    }

    /**
     * set up functional (parametric) dependencies among operations
     *
     * @param operation
     * @param request
     */
    public void setupOperationTDFD(Operation operation, Request request) {
        // TD
        OperationChain oc = addOperationToChain(operation);
        if (operation.isNonDeterministicOperation) {
            checkDependencyForNonDeterministicStateAccess(operation);
        } else {
            // FD
            if (request.condition_keys != null)
                checkFD(oc, operation, request.d_table, request.d_key, request.condition_tables, request.condition_keys);
        }
    }

    /**
     * During the first explore, we will group operations that can be executed sequentially following Temporal dependencies
     * And all groups constructs a group graph.
     * And then partition the group graph by cutting logical dependency edges.
     * And then sorting operations in each partition to execute sequentially on each thread, such that they can be batched for execution.
     *
     * @param context
     */
    public void firstTimeExploreTPG(Context context) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(threadId);

        if (context instanceof OPSContext) {
            ArrayDeque<Operation> roots = new ArrayDeque<>();
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.addNonOperation(this.NonOperations.get(oc.getTableName()));
                    oc.updateDependencies();
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.initializeDependencies();
                    Operation head = oc.getOperations().first();
                    if (head.isRoot()) {
                        roots.add(head);
                    }
                    context.operations.addAll(oc.getOperations());
                    context.totalOsToSchedule += oc.getOperations().size();
                    if (this.isLogging == LOGOption_path) {
                        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                        this.threadToPathRecord.get(context.thisThreadId).addNode(oc.getTableName(), oc.getPrimaryKey(), oc.getOperations().size());
                        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                    }
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            ((OPSContext) context).buildBucketPerThread(context.operations, roots);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            if (context.thisThreadId == 0) { // gather
                for (Context curContext : threadToContextMap.values()) {
                    if (((OPSContext) curContext).maxLevel > maxLevel) {
                        maxLevel = ((OPSContext) curContext).maxLevel;
                    }
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            ((OPSContext) context).maxLevel = maxLevel; // scatter
            if (enable_log) log.info("MaxLevel:" + (((OPSContext) context).maxLevel));
        } else if (context instanceof OPNSContext) {
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.addNonOperation(this.NonOperations.get(oc.getTableName()));
                    oc.updateDependencies();
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.initializeDependencies();
                    Operation head = oc.getOperations().first();
                    context.totalOsToSchedule += oc.getOperations().size();
                    if (head.isRoot()) {
                        head.context.getListener().onRootStart(head);
                    }
                    if (this.isLogging == LOGOption_path) {
                        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                        this.threadToPathRecord.get(context.thisThreadId).addNode(oc.getTableName(), oc.getPrimaryKey(), oc.getOperations().size());
                        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(context.thisThreadId);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }

        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    public void secondTimeExploreTPG(Context context) {
        context.redo();
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            if (!oc.getOperations().isEmpty()) {
                resetOp(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
        if (context instanceof OPSContext) {
            if (enable_log) log.info("MaxLevel:" + (((OPSContext) context).maxLevel));
        } else if (context instanceof OPNSContext) {
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    Operation head = oc.getOperations().first();
                    if (head.isRoot()) {
                        head.context.getListener().onRootStart(head);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void resetOp(OperationChain oc) {
        for (Operation op : oc.getOperations()) {
            op.resetDependencies();
            op.stateTransition(MetaTypes.OperationStateType.BLOCKED);
        }
    }

    /**
     * @param operation
     */
    private OperationChain addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.pKey;
        OperationChain retOc = getOC(table_name, primaryKey, operation.context.thisThreadId);
        retOc.addOperation(operation);
        return retOc;
    }

    private OperationChain getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
//        return holder.get(pKey);
    }

    private OperationChain getOC(String tableName, String pKey, int threadId) {
//        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
//        return holder.get(pKey);
    }

    private void checkFD(OperationChain curOC, Operation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;// no need to check data dependency on a key itself.
                OperationChain OCFromConditionSource = getOC(condition_sourceTable[index], condition_source[index]);
                OCFromConditionSource.addPotentialFDChildren(op);
            }
        }
    }

    private void checkDependencyForNonDeterministicStateAccess(Operation op) {
        //Add Non-deterministic state access operation to all its potential parents
        NonOperations.get(op.table_name).add(op);
    }
}
