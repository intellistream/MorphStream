package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.context.AbstractGSTPGContext;
import scheduler.context.LayeredTPGContext;
import scheduler.context.OCSchedulerContext;
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
    private int NUM_ITEMS;
    private final ConcurrentHashMap<Integer, ConcurrentSkipListSet<ExecutionUnit>> sortedOperations;//shared data structure.
    private final ConcurrentHashMap<String, TableOCs<SchedulingUnit>> operationChains;//shared data structure.
    private final HashMap<Integer, Deque<SchedulingUnit>> threadToOCs;
    CyclicBarrier barrier;

    public void reset(Context context) {
        //reset holder.
//        operationChains = new ConcurrentHashMap<>();
//        operationChains.put("accounts", new TableOCs<>(totalThreads));
//        operationChains.put("bookEntries", new TableOCs<>(totalThreads));
//        sortedOperations.clear();
        operationChains.get("accounts").threadOCsMap.remove(context.thisThreadId);
        operationChains.get("bookEntries").threadOCsMap.remove(context.thisThreadId);
        threadToOCs.remove(context.thisThreadId);
        sortedOperations.remove(context.thisThreadId);
    }

    /**
     * @param totalThreads
     * @param delta
     */
    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS) {
        barrier = new CyclicBarrier(totalThreads);
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs<>(totalThreads));
        operationChains.put("bookEntries", new TableOCs<>(totalThreads));
        threadToContextMap = new HashMap<>();
        threadToOCs = new HashMap<>();
        sortedOperations = new ConcurrentHashMap<>();
    }

    /**
     * Pre-create a bunch of OCs for each key in the table, which reduces the constant overhead during the runtime.
     * @param context
     */
    // TODO: if running multiple batches, this will be a problem.
    public void setOCs(Context context) {
        ArrayDeque<SchedulingUnit> ocs = new ArrayDeque<>();
        int left_bound = context.thisThreadId * delta;
        int right_bound;
        if (context.thisThreadId == totalThreads - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (context.thisThreadId + 1) * delta;
        }
        String _key;
        for (int key = left_bound; key < right_bound; key++) {
            _key = String.valueOf(key);
            SchedulingUnit accOC = context.createTask("accounts", _key, 0);
            SchedulingUnit beOC = context.createTask("bookEntries", _key, 0);
            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, accOC);
            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, beOC);
            ocs.add(accOC);
            ocs.add(beOC);
        }
        threadToOCs.put(context.thisThreadId, ocs);
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

    public SchedulingUnit setupOperationTDFD(ExecutionUnit operation, Context targetContext) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation, targetContext.thisThreadId);
        // FD
        if (operation.condition_source != null)
            checkFD(oc, operation, operation.table_name, operation.d_record.record_.GetPrimaryKey(), operation.condition_sourceTable, operation.condition_source);
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
//        assert context.totalOsToSchedule == ocs.size();
        submit(context, threadToOCs.get(context.thisThreadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void submit(Context context, Collection<SchedulingUnit> ocs) {
        MeasureTools.BEGIN_CACHE_OPERATION_TIME_MEASURE(context.thisThreadId);
        HashSet<OperationChain<ExecutionUnit>> scannedOCs = new HashSet<>();
        HashSet<OperationChain<ExecutionUnit>> circularOCs = new HashSet<>();
        // TODO: simple dfs to solve circular, more efficient algorithm need to be involved. keywords: 如何找出有向图中的所有环路？
//        HashMap<SchedulingUnit, Integer> dfn = new HashMap<>();
//        HashMap<SchedulingUnit, Integer> low = new HashMap<>();
//        HashMap<SchedulingUnit, Boolean> inStack = new HashMap<>();
//        Stack<SchedulingUnit> stack = new Stack<>();
//        int ts = 1;
//        for (SchedulingUnit oc : ocs) {
//            detectCircular(oc, dfn, low, inStack, stack, ts, circularOCs);
//        }
//        detectAffectedOCs(scannedOCs, circularOCs);
        for (SchedulingUnit oc : ocs) {
            detectAffectedOCs(scannedOCs, circularOCs, oc);
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        MeasureTools.END_CACHE_OPERATION_TIME_MEASURE(context.thisThreadId);
        int counter = 0;
        HashSet<OperationChain<ExecutionUnit>> resolvedOC = new HashSet<>();
        for (OperationChain<ExecutionUnit> oc : circularOCs) {
            if (Integer.parseInt(oc.primaryKey) / delta == context.thisThreadId) {
                oc.ocParentsCount.set(0);
                oc.ocParents.clear();
                oc.ocChildren.clear();
                resolvedOC.add(oc);
                counter++;
            }
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
                if (oc.getOperations().isEmpty()) continue;
                context.totalOsToSchedule += oc.getOperations().size();
//                context.operationChains.add(oc);
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

    private void detectCircular(SchedulingUnit oc,
                                   HashMap<SchedulingUnit, Integer> dfn,
                                   HashMap<SchedulingUnit, Integer> low,
                                   HashMap<SchedulingUnit, Boolean> inStack,
                                   Stack<SchedulingUnit> stack,
                                   int ts,
                                   HashSet<OperationChain<ExecutionUnit>> circularOCs) {
        if (!oc.getOperations().isEmpty() && !dfn.containsKey(oc)) {
            tarjanDfs(oc, dfn, low, inStack, stack, ts, circularOCs);
        }
    }

    private void tarjanDfs(SchedulingUnit oc,
                           HashMap<SchedulingUnit, Integer> dfn,
                           HashMap<SchedulingUnit, Integer> low,
                           HashMap<SchedulingUnit, Boolean> inStack,
                           Stack<SchedulingUnit> stack,
                           int ts,
                           HashSet<OperationChain<ExecutionUnit>> circularOCs) {
        dfn.put(oc, ts);
        low.put(oc, ts);
        ts++;
        stack.push(oc);
        inStack.put(oc, true);

//        for (OperationChain<ExecutionUnit> childOC : oc.ocChildren.keySet()) {
//            if (!dfn.containsKey(childOC)) {
//                tarjanDfs((SchedulingUnit) childOC, dfn, low, inStack, stack, ts, circularOCs);
//                low.put(oc, Math.min(low.get(oc), low.get(childOC)));
//            } else if (inStack.get(childOC)) {
//                low.put(oc, Math.min(low.get(oc), dfn.get(childOC)));
//            }
//        }

        for (OperationChain<ExecutionUnit> parentOC : oc.ocParents.keySet()) {
            if (!dfn.containsKey(parentOC)) {
                tarjanDfs((SchedulingUnit) parentOC, dfn, low, inStack, stack, ts, circularOCs);
                low.put(oc, Math.min(low.get(oc), low.get(parentOC)));
            } else if (inStack.get(parentOC)) {
                low.put(oc, Math.min(low.get(oc), dfn.get(parentOC)));
            }
        }

        if (dfn.get(oc).equals(low.get(oc))) {
            SchedulingUnit tmp;
            List<SchedulingUnit> scc = new ArrayList<>();
            do {
                tmp = stack.pop();
                inStack.put(tmp, false);
                scc.add(tmp);
            } while (tmp != oc);
            if (scc.size() > 1) {
                circularOCs.addAll(scc);
            }
        }
    }

    /**
     * Scan from the circular OC and find all the children to resolve circular
     * @param affectedOCs
     * @param circularOCs
     */
    private void detectAffectedOCs(HashSet<OperationChain<ExecutionUnit>> affectedOCs, HashSet<OperationChain<ExecutionUnit>> circularOCs) {
        for (OperationChain<ExecutionUnit> circularOC : circularOCs) {
            dfs((SchedulingUnit) circularOC, affectedOCs);
        }
    }

    public void dfs(SchedulingUnit oc, HashSet<OperationChain<ExecutionUnit>> affectedOCs) {
        affectedOCs.add(oc);
        for (OperationChain<ExecutionUnit> childOC : oc.ocChildren.keySet()) {
            if (!affectedOCs.contains(childOC)) {
                dfs((SchedulingUnit) childOC, affectedOCs);
            }
        }
    }

    private void detectAffectedOCs(HashSet<OperationChain<ExecutionUnit>> scannedOCs,
                                   HashSet<OperationChain<ExecutionUnit>> circularOCs,
                                   SchedulingUnit oc) {
        if (!oc.getOperations().isEmpty() && oc.hasParents()) {
//            for (OperationChain<ExecutionUnit> parent :oc.ocParents.keySet()) {
//                if (circularOCs.contains(parent)) {
//                    circularOCs.add(oc);
//                    return;
//                }
//            }
            if (circularOCs.contains(oc)) {
                return;
            }
            scannedOCs.clear();
            scannedOCs.add(oc);
            // scan from leaves and check whether circular are detected.
            oc.isCircularAffected(scannedOCs, circularOCs);
            if (oc.isCircularAffected(scannedOCs, circularOCs)) {
                circularOCs.add(oc);
//                circularOCs.addAll(scannedOCs);
                dfs(oc, circularOCs);
            }
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

    public SchedulingUnit addOperationToChain(ExecutionUnit operation, int targetThreadId) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        SchedulingUnit retOc = getOC(table_name, primaryKey, targetThreadId);
        retOc.addOperation(operation);
        return retOc;
    }

    private SchedulingUnit getOC(String tableName, String pKey, int threadId) {
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
//        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
        return holder.get(pKey);
    }

    private SchedulingUnit getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.get(pKey);
    }

//    private SchedulingUnit getOC(String tableName, String pKey) {
//        int threadId = Integer.parseInt(pKey) / delta;
//        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
//        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
//    }

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
