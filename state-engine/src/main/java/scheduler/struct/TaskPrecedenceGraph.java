package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.AbstractGSTPGContext;
import scheduler.context.GSTPGContext;
import scheduler.context.LayeredTPGContext;
import scheduler.context.OCSchedulerContext;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.struct.gs.AbstractGSOperationChain;
import transaction.impl.ordered.MyList;
import utils.AppConfig;
import utils.SOURCE_CONTROL;
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.CyclicBarrier;

import static common.CONTROL.enable_log;

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
    public final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final ConcurrentHashMap<String, TableOCs<SchedulingUnit>> operationChains;//shared data structure.
    private final ConcurrentHashMap<Integer, Deque<SchedulingUnit>> threadToOCs;
    CyclicBarrier barrier;
    private int maxLevel = 0; // just for layered scheduling
    private final int app;

    public void reset(Context context) {
        //reset holder.
        if (app == 0) {
            operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 1) {
            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        }
//        threadToOCs.get(context.thisThreadId).clear();
        for (SchedulingUnit oc : threadToOCs.get(context.thisThreadId)) {
            oc.clear();
        }
//        this.setOCs(context);
    }

    /**
     * @param totalThreads
     * @param delta
     * @param app
     */
    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS, int app) {
        barrier = new CyclicBarrier(totalThreads);
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        threadToContextMap = new HashMap<>();
        threadToOCs = new ConcurrentHashMap<>();
        //shared data structure.
        this.app = app;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        if (app == 0) {
            operationChains.put("MicroTable", new TableOCs<>(totalThreads));
        } else if (app == 1) {
            operationChains.put("accounts", new TableOCs<>(totalThreads));
            operationChains.put("bookEntries", new TableOCs<>(totalThreads));
        }
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
            if (app == 0) {
                SchedulingUnit gsOC = context.createTask("MicroTable", _key, 0);
                operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            } else if (app == 1) {
                SchedulingUnit accOC = context.createTask("accounts", _key, 0);
                SchedulingUnit beOC = context.createTask("bookEntries", _key, 0);
                operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, accOC);
                operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, beOC);
                ocs.add(accOC);
                ocs.add(beOC);
            }
        }
        threadToOCs.put(context.thisThreadId, ocs);
    }

    public TableOCs<SchedulingUnit> getTableOCs(String table_name) {
        return operationChains.get(table_name);
    }

    public ConcurrentHashMap<String, TableOCs<SchedulingUnit>> getOperationChains() {
        return operationChains;
    }

    public void setupOperationTDFD(ExecutionUnit operation, Request request, Context targetContext) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation, targetContext.thisThreadId);
        // FD
        if (request.condition_source != null)
            checkFD(oc, operation, operation.table_name, operation.d_record.record_.GetPrimaryKey(), request.condition_sourceTable, request.condition_source);
    }



    public void firstTimeExploreTPG(Context context) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(threadId);
//        assert context.totalOsToSchedule == ocs.size();
//        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
//        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
//            threadToOCs.computeIfAbsent(threadId, s -> new ArrayDeque<>()).addAll(tableOCs.threadOCsMap.get(threadId).holder_v1.values());
//        }

        submit(context, threadToOCs.get(threadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(threadId);
    }

    private void submit(Context context, Collection<SchedulingUnit> ocs) {
        HashSet<OperationChain<ExecutionUnit>> scannedOCs = new HashSet<>();
        HashSet<OperationChain<ExecutionUnit>> circularOCs = new HashSet<>();
        HashSet<OperationChain<ExecutionUnit>> resolvedOC = new HashSet<>();
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
        if (AppConfig.isCyclic) { // if the constructed OCs are not cyclic, skip this.
            circularDetect(context, ocs, scannedOCs, circularOCs, resolvedOC);
        }
//        for (SchedulingUnit oc : ocs) {
//            context.fd += oc.ocParentsCount.get();
//        }
//        LOG.info("id: " + context.thisThreadId + " fd: " + context.fd);
        if (context instanceof LayeredTPGContext) {
            ((LayeredTPGContext) context).buildBucketPerThread(ocs, resolvedOC);
            if (AppConfig.isCyclic) { // if the constructed OCs are not cyclic, skip this.
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                if (context.thisThreadId == 0) {
                    for (Context curContext : threadToContextMap.values()) {
                        if (((LayeredTPGContext) curContext).maxLevel > maxLevel) {
                            maxLevel = ((LayeredTPGContext) curContext).maxLevel;
                        }
                    }
                }
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                ((LayeredTPGContext) context).putBusyWaitOCs(resolvedOC, maxLevel);
            }
            if (enable_log) LOG.info("MaxLevel:" + (((LayeredTPGContext) context).maxLevel));
        } else if (context instanceof AbstractGSTPGContext) {
            for (SchedulingUnit oc : ocs) {
                if (oc.getOperations().isEmpty()) {
                    continue;
                }
                context.totalOsToSchedule += oc.getOperations().size();
                context.operationChains.add(oc);
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

    private void circularDetect(Context context, Collection<SchedulingUnit> ocs, HashSet<OperationChain<ExecutionUnit>> scannedOCs, HashSet<OperationChain<ExecutionUnit>> circularOCs, HashSet<OperationChain<ExecutionUnit>> resolvedOC) {
        for (SchedulingUnit oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                context.fd += oc.ocParentsCount.get();
                detectAffectedOCs(scannedOCs, circularOCs, oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        int counter = 0;
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
    }


    public void secondTimeExploreTPG(Context context) {
        context.redo();
        for (SchedulingUnit oc : threadToOCs.get(context.thisThreadId)) {
            if (!oc.getOperations().isEmpty()) {
                resetOC(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
        if (context instanceof LayeredTPGContext) {
            if (enable_log) LOG.info("MaxLevel:" + (((LayeredTPGContext) context).maxLevel));
        } else if (context instanceof GSTPGContext) {
            for (SchedulingUnit oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    if (!((AbstractGSOperationChain) oc).context.equals(context)) {
                        throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
                    }
                    if (!oc.hasParents()) {
                        ((AbstractGSTPGContext) context).getListener().onOcRootStart(oc);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void resetOC(SchedulingUnit oc) {
        oc.reset();
        for (ExecutionUnit op : oc.getOperations()) {
            op.stateTransition(MetaTypes.OperationStateType.BLOCKED);
            if (op.isFailed) { // transit state to aborted.
                op.stateTransition(MetaTypes.OperationStateType.ABORTED);
            }
        }
    }

    public void tStreamExplore(Context context) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(threadId);
//        assert context.totalOsToSchedule == ocs.size();
//        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
//        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
//            threadToOCs.computeIfAbsent(threadId, s -> new ArrayDeque<>()).addAll(tableOCs.threadOCsMap.get(threadId).holder_v1.values());
//        }

        tStreamSubmit(context, threadToOCs.get(threadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void tStreamSubmit(Context context, Collection<SchedulingUnit> ocs) {
        ArrayDeque<SchedulingUnit> nonNullOCs = new ArrayDeque<>();
        HashSet<OperationChain<ExecutionUnit>> circularOCs = new HashSet<>();
        for (SchedulingUnit oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                nonNullOCs.add(oc);
//                context.fd += oc.ocParentsCount.get();
                circularOCs.add(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        int counter = 0;
        for (OperationChain<ExecutionUnit> oc : circularOCs) {
            if (Integer.parseInt(oc.primaryKey) / delta == context.thisThreadId) {
                oc.ocParentsCount.set(0);
                oc.ocParents.clear();
                oc.ocChildren.clear();
                counter++;
            }
        }
        if (enable_log) LOG.info(context.thisThreadId + " : " + counter);
//        LOG.info("fd number: " + context.fd);
        assert context instanceof AbstractGSTPGContext;
        for (SchedulingUnit oc : nonNullOCs) {
            context.totalOsToSchedule += oc.getOperations().size();
            context.operationChains.add(oc);
            if (!((AbstractGSOperationChain) oc).context.equals(context)) {
                throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
            }
            if (!oc.hasParents()) {
                ((AbstractGSTPGContext) context).getListener().onOcRootStart(oc);
            }
        }
        if (enable_log) LOG.info("average length of oc:" + context.totalOsToSchedule / nonNullOCs.size());
    }


    public void tStreamReExplore(Context context) {
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
//        assert context.totalOsToSchedule == ocs.size();
        tStreamReSubmit(context, threadToOCs.get(context.thisThreadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void tStreamReSubmit(Context context, Collection<SchedulingUnit> ocs) {
        context.redo();
        ArrayDeque<SchedulingUnit> nonNullOCs = new ArrayDeque<>();
        HashSet<OperationChain<ExecutionUnit>> circularOCs = new HashSet<>();
        for (SchedulingUnit oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                resetOC(oc);
                nonNullOCs.add(oc);
                circularOCs.add(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        assert context instanceof AbstractGSTPGContext;
        for (SchedulingUnit oc : nonNullOCs) {
            context.operationChains.add(oc);
            if (!((AbstractGSOperationChain) oc).context.equals(context)) {
                throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
            }
            if (!oc.hasParents()) {
                ((AbstractGSTPGContext) context).getListener().onOcRootStart(oc);
            }
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
        if (oc.hasParents()) {
            if (circularOCs.contains(oc)) {
                return;
            }
            scannedOCs.clear();
            scannedOCs.add(oc);
            // scan from leaves and check whether circular are detected.
//            oc.isCircularAffected(scannedOCs, circularOCs);
            if (oc.isCircularAffected(scannedOCs, circularOCs)) {
                circularOCs.add(oc);
                dfs(oc, circularOCs);
            }
        }
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
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
//        return holder.get(pKey);
    }

    private SchedulingUnit getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
//        return holder.get(pKey);
    }

    private void checkFD(SchedulingUnit curOC, ExecutionUnit op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
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
    }

    public Collection<Context> getContexts() {
        return threadToContextMap.values();
    }
}
