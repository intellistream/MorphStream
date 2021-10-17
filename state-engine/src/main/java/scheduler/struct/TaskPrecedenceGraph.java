package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.AbstractGSTPGContext;
import scheduler.context.LayeredTPGContext;
import scheduler.context.OCSchedulerContext;
import scheduler.oplevel.struct.Operation;
import scheduler.struct.gs.AbstractGSOperationChain;
import transaction.impl.ordered.MyList;
import utils.SOURCE_CONTROL;
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;

import static common.CONTROL.enable_log;
import static scheduler.impl.OCScheduler.getTaskId;

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
public class TaskPrecedenceGraph<Context extends OCSchedulerContext<SchedulingUnit>, SchedulingUnit extends OperationChain<ExecutionUnit>, ExecutionUnit extends AbstractOperation> {
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final Map<Integer, Context> threadToContextMap;
    private final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final ConcurrentHashMap<Integer, ConcurrentSkipListSet<ExecutionUnit>> sortedOperations;//shared data structure.
    private final ConcurrentHashMap<String, TableOCs<SchedulingUnit>> operationChains;//shared data structure.
    CyclicBarrier barrier;

    public void reset(Context context) {
        //reset holder.
//        operationChains = new ConcurrentHashMap<>();
//        operationChains.put("accounts", new TableOCs<>(totalThreads));
//        operationChains.put("bookEntries", new TableOCs<>(totalThreads));
//        sortedOperations.clear();
        operationChains.get("accounts").threadOCsMap.remove(context.thisThreadId);
        operationChains.get("bookEntries").threadOCsMap.remove(context.thisThreadId);
        sortedOperations.remove(context.thisThreadId);
    }

    /**
     * @param totalThreads
     * @param delta
     */
    public TaskPrecedenceGraph(int totalThreads, int delta) {
        barrier = new CyclicBarrier(totalThreads);
        this.totalThreads = totalThreads;
        this.delta = delta;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs<>(totalThreads));
        operationChains.put("bookEntries", new TableOCs<>(totalThreads));
        threadToContextMap = new HashMap<>();
        sortedOperations = new ConcurrentHashMap<>();
    }

    public TableOCs<SchedulingUnit> getTableOCs(String table_name) {
        return operationChains.get(table_name);
    }

    public ConcurrentHashMap<String, TableOCs<SchedulingUnit>> getOperationChains() {
        return operationChains;
    }

    /**
     * set up functional dependencies among operations
     *
     * @param operation
     */
    public SchedulingUnit setupOperationTDFD(ExecutionUnit operation) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation);
        // FD
        if (operation.condition_source != null)
            checkFD(oc, operation, operation.table_name, operation.d_record.record_.GetPrimaryKey(), operation.condition_sourceTable, operation.condition_source);
        return oc;
    }

    /**
     * set up functional dependencies among operations
     *
     * @param operation
     * @param request
     */
    public SchedulingUnit setupOperationTDFD(ExecutionUnit operation, Request request) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation);
        // FD
        if (request.condition_source != null)
            checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
        return oc;
    }

    /**
     * construct TPG from the arrived sorted operations, typically this step is to set dependencies and solve circular
     * @param context
     */
    public void constructTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        for (ExecutionUnit op : sortedOperations.get(threadId)) {
            SchedulingUnit oc = addOperationToChain(op);
            if (op.condition_source != null)
                checkFD(oc, op, op.table_name, op.d_record.record_.GetPrimaryKey(), op.condition_sourceTable, op.condition_source);
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    public void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        ArrayList<SchedulingUnit> ocs = new ArrayList<>();
        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
            ocs.addAll(tableOCs.threadOCsMap.get(threadId).holder_v1.values());
        }
//        assert context.totalOsToSchedule == ocs.size();
        submit(context, ocs);
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void submit(Context context, Collection<SchedulingUnit> ocs) {
        ArrayDeque<OperationChain<ExecutionUnit>> scannedOC = new ArrayDeque<>();
        ArrayDeque<OperationChain<ExecutionUnit>> resolvedOC = new ArrayDeque<>();
        // TODO: simple dfs to solve circular, more efficient algorithm need to be involved. keywords: 如何找出有向图中的所有环路？
        for (SchedulingUnit oc : ocs) {
            resolveCircular(scannedOC, resolvedOC, oc);
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        int counter = 0;
        for (OperationChain<ExecutionUnit> oc : resolvedOC) {
            assert  getTaskId(oc.primaryKey, delta) == context.thisThreadId;
            oc.ocParentsCount.set(0);
            oc.ocParents.clear();
            oc.ocChildren.clear();
            counter++;
        }
        if (enable_log) LOG.info(context.thisThreadId + " : " + counter);
        if (context instanceof LayeredTPGContext) {
            for (SchedulingUnit oc : ocs) {
                context.totalOsToSchedule += oc.getOperations().size();
            }
            ((LayeredTPGContext) context).buildBucketPerThread(ocs, resolvedOC);
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            int maxLevel = 0;
            for (Context curContext : threadToContextMap.values()) {
                if (((LayeredTPGContext) curContext).maxLevel > maxLevel) {
                    maxLevel = ((LayeredTPGContext) curContext).maxLevel;
                }
            }
            ((LayeredTPGContext) context).putBusyWaitOCs(resolvedOC, maxLevel+1);
            if (enable_log) LOG.info("MaxLevel:" + (((LayeredTPGContext) context).maxLevel));
        } else if (context instanceof AbstractGSTPGContext) {
            for (SchedulingUnit oc : ocs) {
                context.totalOsToSchedule += oc.getOperations().size();
//                context.operaitonsLeft.addAll(oc.getOperations());
                if (!((AbstractGSOperationChain) oc).context.equals(context)) {
                    throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
                }
                if (!oc.hasParents()) {
                    ((AbstractGSTPGContext) context).getListener().onOcRootStart(oc);
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported.");
        }
        if (enable_log) LOG.info("average length of oc:" + context.totalOsToSchedule / ocs.size());
    }

    private void resolveCircular(ArrayDeque<OperationChain<ExecutionUnit>> scannedOC,
                                 ArrayDeque<OperationChain<ExecutionUnit>> resolvedOC,
                                 SchedulingUnit oc) {
        scannedOC.clear();
        scannedOC.add(oc);
        // scan from leaves and check whether circular are detected.
        if (oc.isCircularAffected(scannedOC)) {
            resolvedOC.add(oc);
        }
    }

    /**
     * @param operation
     */
    public void cacheToSortedOperations(ExecutionUnit operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        int threadId = getTaskId(primaryKey, delta);
        ConcurrentSkipListSet<ExecutionUnit> operations = sortedOperations.computeIfAbsent(threadId, s -> new ConcurrentSkipListSet<>());
        operations.add(operation);
        createQueueForState(operation.table_name, operation.d_record.record_.GetPrimaryKey());
    }

    private void createQueueForState(String tableName, String pKey) {
        int threadId = getTaskId(pKey, delta);
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        holder.computeIfAbsent(pKey, s -> {
            SchedulingUnit oc = threadToContextMap.get(threadId).createTask(tableName, pKey, 0);
            oc.setupTPG(this);
            return oc;
        });
    }

    /**
     * @param operation
     */
    public SchedulingUnit addOperationToChain(ExecutionUnit operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        SchedulingUnit retOc = getOC(table_name, primaryKey);
        retOc.addOperation(operation);
        return retOc;
    }

    private SchedulingUnit getOC(String tableName, String pKey) {
        int threadId = getTaskId(pKey, delta);
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
    }

//    private SchedulingUnit getOC(String tableName, String pKey) {
//        int threadId = getTaskId(pKey, delta);
//        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
//        return holder.get(pKey);
//    }

//    /**
//     * create a new oc for a circular oc partition
//     * @param tableName
//     * @param pKey
//     * @param bid
//     * @return
//     */
//    public SchedulingUnit getNewOC(String tableName, String pKey, long bid) {
//        if (enable_log) LOG.info("Circular OC that has been splitted: " + tableName + "|" + pKey + "|" + bid);
//        int threadId = getTaskId(pKey, delta);
//        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
//        SchedulingUnit oc = threadToContextMap.get(threadId).createTask(tableName, pKey, bid);
//        SchedulingUnit oldOC = holder.put(pKey, oc);
//        assert oldOC == null || oldOC.bid <= oc.bid;
//        oc.setupTPG(this);
//        return oc;
//    }

    public SchedulingUnit getNewOC(String tableName, String pKey, long bid) {
        if (enable_log) LOG.info("Circular OC that has been splitted: " + tableName + "|" + pKey + "|" + bid);
        int threadId = getTaskId(pKey, delta);
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        SchedulingUnit newOC = threadToContextMap.get(threadId).createTask(tableName, pKey, bid);
        SchedulingUnit curOC = holder.put(pKey, newOC);
        assert curOC == null || curOC.bid <= newOC.bid;
        newOC.setupTPG(this);
        return newOC;
    }

//    private void checkFD(ExecutionUnit op, String table_name,
//                         String key, String[] condition_sourceTable, String[] condition_source) {
//        for (int index = 0; index < condition_source.length; index++) {
//            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
//                continue;// no need to check data dependency on a key itself.
//            // TODO: this can be optimized by checking version only rather than query on the entire TPG
//            SchedulingUnit curOC = getOC(table_name, key); // the oc should can be either the splitted oc or the original oc to check potentialFD
//            SchedulingUnit OCFromConditionSource = getOC(condition_sourceTable[index], condition_source[index]);
//            MyList<ExecutionUnit> conditionedOps = OCFromConditionSource.getOperations();
//            if (OCFromConditionSource.getOperations().isEmpty() || conditionedOps.first().bid >= op.bid) {
//                OCFromConditionSource.addPotentialFDChildren(curOC, op);
//            } else {
//                // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
//                curOC.addParent(op, OCFromConditionSource); // record dependency
//            }
//        }
//        SchedulingUnit ocToCheckPotential = getOC(table_name, key); // the oc can be either the splitted oc or the original oc to check potentialFD
//        ocToCheckPotential.checkPotentialFDChildrenOnNewArrival(op);
//    }
    private void checkFD(SchedulingUnit curOC, ExecutionUnit op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            SchedulingUnit OCFromConditionSource = getOC(condition_sourceTable[index], condition_source[index]);
            // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
            MyList<ExecutionUnit> conditionedOps = OCFromConditionSource.getOperations();
            if (OCFromConditionSource.getOperations().isEmpty() || conditionedOps.first().bid >= op.bid) {
                OCFromConditionSource.addPotentialFDChildren(curOC, op);
            } else {
                // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                curOC.addParent(op, OCFromConditionSource); // record dependency
            }
        }
        curOC.checkPotentialFDChildrenOnNewArrival(op);
    }

//    public SchedulingUnit forceExecuteBlockedOC(Context context) {
//        int threadId = context.thisThreadId;
//        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
//        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
//            for (Deque<SchedulingUnit> ocsPerKey : tableOCs.threadOCsMap.get(threadId).holder_v1.values()) {
//                for (SchedulingUnit oc : ocsPerKey) { // TODO: this part can be buggy for correctness
//                    if (!oc.isExecuted && !context.busyWaitQueue.contains(oc)) {
//                        return oc;
//                    }
//                }
//            }
//        }
//        return null;
//    }

    public Collection<Context> getContexts() {
        return threadToContextMap.values();
    }
}
