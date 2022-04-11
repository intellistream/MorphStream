package scheduler.struct.og;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.og.AbstractOGNSContext;
import scheduler.context.og.OGNSContext;
import scheduler.context.og.OGSContext;
import scheduler.context.og.OGSchedulerContext;
import scheduler.struct.op.MetaTypes;
import transaction.TxnManager;
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
public class TaskPrecedenceGraph<Context extends OGSchedulerContext> {
    private static final Logger log = LoggerFactory.getLogger(TaskPrecedenceGraph.class);

    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    public final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final ConcurrentHashMap<String, TableOCs<OperationChain>> operationChains;//shared data structure.
    private final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;
    CyclicBarrier barrier;
    private int maxLevel = 0; // just for layered scheduling
    private final int app;

    public void reset(Context context) {
//        //reset holder.
//        if (app == 0) {
//            operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 1) {
//            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 2 ) {
//            operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//            operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 3) {
//            operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        }
//        threadToOCs.get(context.thisThreadId).clear();
//        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
//            oc.clear();
//        }
//        this.setOCs(context);
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            oc.clear(); // only need to clear all operations from all ocs
        }
        log.info("===Clear current data for the next batch===");
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
        threadToContextMap = new ConcurrentHashMap<>();
        threadToOCs = new ConcurrentHashMap<>();
        //shared data structure.
        this.app = app;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
    }
    public void initTPG(int offset) {
        if (app == 0) {//GS
            operationChains.put("MicroTable", new TableOCs<>(totalThreads,offset));
        } else if (app == 1) {//SL
            operationChains.put("accounts", new TableOCs<>(totalThreads,offset));
            operationChains.put("bookEntries", new TableOCs<>(totalThreads,offset));
        } else if(app == 2) {//TP
            operationChains.put("segment_speed",new TableOCs<>(totalThreads,offset));
            operationChains.put("segment_cnt",new TableOCs<>(totalThreads,offset));
        } else if (app == 3) {
            operationChains.put("goods",new TableOCs<>(totalThreads,offset));
        }
    }

    /**
     * Pre-create a bunch of OCs for each key in the table, which reduces the constant overhead during the runtime.
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
        String _key;
        resetOCs(context);
        for (int key = left_bound; key < right_bound; key++) {
            _key = String.valueOf(key);
            if (app == 0) {
                OperationChain gsOC = context.createTask("MicroTable", _key, 0);
                operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            } else if (app == 1) {
                OperationChain accOC = context.createTask("accounts", _key, 0);
                OperationChain beOC = context.createTask("bookEntries", _key, 0);
                operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, accOC);
                operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, beOC);
                ocs.add(accOC);
                ocs.add(beOC);
            } else if( app == 2) {
                OperationChain speedOC=context.createTask("segment_speed",_key,0);
                OperationChain cntOC=context.createTask("segment_cnt",_key,0);
                operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, speedOC);
                operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, cntOC);
                ocs.add(speedOC);
                ocs.add(cntOC);
            } else if (app == 3){
                OperationChain gsOC = context.createTask("goods", _key, 0);
                operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            }
        }
        threadToOCs.put(context.thisThreadId, ocs);
    }
    private void resetOCs(Context context) {
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

    public TableOCs<OperationChain> getTableOCs(String table_name) {
        return operationChains.get(table_name);
    }

    public ConcurrentHashMap<String, TableOCs<OperationChain>> getOperationChains() {
        return operationChains;
    }

    public void setupOperationTDFD(Operation operation, Request request, Context targetContext) {
        // TD
        OperationChain oc = addOperationToChain(operation, targetContext.thisThreadId);
        // FD
        if (request.condition_source != null)
            checkFD(oc, operation, operation.table_name, operation.d_record.record_.GetPrimaryKey(), request.condition_sourceTable, request.condition_source);
    }



    public void firstTimeExploreTPG(Context context) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(threadId);
//        assert context.totalOsToSchedule == ocs.size();
//        Collection<TableOCs<OperationChain>> tableOCsList = getOperationChains().values();
//        for (TableOCs<OperationChain> tableOCs : tableOCsList) {//for each table.
//            threadToOCs.computeIfAbsent(threadId, s -> new ArrayDeque<>()).addAll(tableOCs.threadOCsMap.get(threadId).holder_v1.values());
//        }

        submit(context, threadToOCs.get(threadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(threadId);
    }

    private void submit(Context context, Collection<OperationChain> ocs) {
        HashSet<OperationChain> scannedOCs = new HashSet<>();
        HashSet<OperationChain> circularOCs = new HashSet<>();
        HashSet<OperationChain> resolvedOC = new HashSet<>();
        // TODO: simple dfs to solve circular, more efficient algorithm need to be involved. keywords: 如何找出有向图中的所有环路？
//        HashMap<OperationChain, Integer> dfn = new HashMap<>();
//        HashMap<OperationChain, Integer> low = new HashMap<>();
//        HashMap<OperationChain, Boolean> inStack = new HashMap<>();
//        Stack<OperationChain> stack = new Stack<>();
//        int ts = 1;
//        for (OperationChain oc : ocs) {
//            detectCircular(oc, dfn, low, inStack, stack, ts, circularOCs);
//        }
//        detectAffectedOCs(scannedOCs, circularOCs);
        if (AppConfig.isCyclic) { // if the constructed OCs are not cyclic, skip this.
            circularDetect(context, ocs, scannedOCs, circularOCs, resolvedOC);
        }
//        for (OperationChain oc : ocs) {
//            context.fd += oc.ocParentsCount.get();
//        }
//        LOG.info("id: " + context.thisThreadId + " fd: " + context.fd);
        if (context instanceof OGSContext) {
            ((OGSContext) context).buildBucketPerThread(ocs, resolvedOC);
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            if (context.thisThreadId == 0) { // gather
                for (Context curContext : threadToContextMap.values()) {
                    if (((OGSContext) curContext).maxLevel > maxLevel) {
                        maxLevel = ((OGSContext) curContext).maxLevel;
                    }
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads();
            ((OGSContext) context).maxLevel = maxLevel; // scatter
            if (AppConfig.isCyclic) { // if the constructed OCs are not cyclic, skip this.
                ((OGSContext) context).putBusyWaitOCs(resolvedOC, maxLevel);
            }
            if (enable_log) LOG.info("MaxLevel:" + (((OGSContext) context).maxLevel));
        } else if (context instanceof AbstractOGNSContext) {
            for (OperationChain oc : ocs) {
                if (oc.getOperations().isEmpty()) {
                    continue;
                }
                context.totalOsToSchedule += oc.getOperations().size();
                context.operationChains.add(oc);
                if (!oc.context.equals(context)) {
                    throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
                }
                if (!oc.hasParents()) {
                    ((AbstractOGNSContext) context).getListener().onOcRootStart(oc);
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported.");
        }
        if (enable_log) LOG.info("total ops to schedule:" + context.totalOsToSchedule);
    }

    private void circularDetect(Context context, Collection<OperationChain> ocs, HashSet<OperationChain> scannedOCs, HashSet<OperationChain> circularOCs, HashSet<OperationChain> resolvedOC) {
        for (OperationChain oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                context.fd += oc.ocParentsCount.get();
                detectAffectedOCs(scannedOCs, circularOCs, oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        int counter = 0;
        for (OperationChain oc : circularOCs) {
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
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            if (!oc.getOperations().isEmpty()) {
                resetOC(oc);
                context.operationChains.add(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
        if (context instanceof OGSContext) {
            if (enable_log) LOG.info("MaxLevel:" + (((OGSContext) context).maxLevel));
        } else if (context instanceof OGNSContext) {
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    if (!oc.context.equals(context)) {
                        throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
                    }
                    if (!oc.hasParents()) {
                        ((AbstractOGNSContext) context).getListener().onOcRootStart(oc);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void resetOC(OperationChain oc) {
        oc.reset();
        for (Operation op : oc.getOperations()) {
            op.stateTransition(MetaTypes.OperationStateType.BLOCKED);
            if (op.isFailed) { // transit state to aborted.
                op.stateTransition(MetaTypes.OperationStateType.ABORTED);
            }
        }
    }

    public void Explore(Context context) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(threadId);
//        assert context.totalOsToSchedule == ocs.size();
//        Collection<TableOCs<OperationChain>> tableOCsList = getOperationChains().values();
//        for (TableOCs<OperationChain> tableOCs : tableOCsList) {//for each table.
//            threadToOCs.computeIfAbsent(threadId, s -> new ArrayDeque<>()).addAll(tableOCs.threadOCsMap.get(threadId).holder_v1.values());
//        }

        Submit(context, threadToOCs.get(threadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void Submit(Context context, Collection<OperationChain> ocs) {
        ArrayDeque<OperationChain> nonNullOCs = new ArrayDeque<>();
        HashSet<OperationChain> circularOCs = new HashSet<>();
        for (OperationChain oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                nonNullOCs.add(oc);
//                context.fd += oc.ocParentsCount.get();
                circularOCs.add(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        int counter = 0;
        for (OperationChain oc : circularOCs) {
            if (Integer.parseInt(oc.primaryKey) / delta == context.thisThreadId) {
                counter ++;
                oc.ocParentsCount.set(0);
                oc.ocParents.clear();
                oc.ocChildren.clear();
            }
        }
        if (enable_log) LOG.info(context.thisThreadId + " : " + counter);
//        LOG.info("fd number: " + context.fd);
        assert context instanceof AbstractOGNSContext;
        for (OperationChain oc : nonNullOCs) {
            context.totalOsToSchedule += oc.getOperations().size();
            context.operationChains.add(oc);
            if (!oc.context.equals(context)) {
                throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
            }
            if (!oc.hasParents()) {
                ((AbstractOGNSContext) context).getListener().onOcRootStart(oc);
            }
        }
        if (enable_log) LOG.info("average length of oc:" + context.totalOsToSchedule / nonNullOCs.size());
    }


    public void ReExplore(Context context) {
        MeasureTools.BEGIN_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
//        assert context.totalOsToSchedule == ocs.size();
        ReSubmit(context, threadToOCs.get(context.thisThreadId));
        MeasureTools.END_FIRST_EXPLORE_TIME_MEASURE(context.thisThreadId);
    }

    private void ReSubmit(Context context, Collection<OperationChain> ocs) {
        context.redo();
        ArrayDeque<OperationChain> nonNullOCs = new ArrayDeque<>();
        HashSet<OperationChain> circularOCs = new HashSet<>();
        for (OperationChain oc : ocs) {
            if (!oc.getOperations().isEmpty()) {
                resetOC(oc);
                nonNullOCs.add(oc);
                circularOCs.add(oc);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(); // wait until all threads find the circular ocs.
        assert context instanceof AbstractOGNSContext;
        for (OperationChain oc : nonNullOCs) {
            context.operationChains.add(oc);
            if (!oc.context.equals(context)) {
                throw new RuntimeException("context of the OC should always be the same as those who submit the OC");
            }
            if (!oc.hasParents()) {
                ((AbstractOGNSContext) context).getListener().onOcRootStart(oc);
            }
        }
        if (enable_log) LOG.info("average length of oc:" + context.totalOsToSchedule / ocs.size());
    }

    private void detectCircular(OperationChain oc,
                                   HashMap<OperationChain, Integer> dfn,
                                   HashMap<OperationChain, Integer> low,
                                   HashMap<OperationChain, Boolean> inStack,
                                   Stack<OperationChain> stack,
                                   int ts,
                                   HashSet<OperationChain> circularOCs) {
        if (!oc.getOperations().isEmpty() && !dfn.containsKey(oc)) {
            tarjanDfs(oc, dfn, low, inStack, stack, ts, circularOCs);
        }
    }

    private void tarjanDfs(OperationChain oc,
                           HashMap<OperationChain, Integer> dfn,
                           HashMap<OperationChain, Integer> low,
                           HashMap<OperationChain, Boolean> inStack,
                           Stack<OperationChain> stack,
                           int ts,
                           HashSet<OperationChain> circularOCs) {
        dfn.put(oc, ts);
        low.put(oc, ts);
        ts++;
        stack.push(oc);
        inStack.put(oc, true);

        for (OperationChain parentOC : oc.ocParents.keySet()) {
            if (!dfn.containsKey(parentOC)) {
                tarjanDfs((OperationChain) parentOC, dfn, low, inStack, stack, ts, circularOCs);
                low.put(oc, Math.min(low.get(oc), low.get(parentOC)));
            } else if (inStack.get(parentOC)) {
                low.put(oc, Math.min(low.get(oc), dfn.get(parentOC)));
            }
        }

        if (dfn.get(oc).equals(low.get(oc))) {
            OperationChain tmp;
            List<OperationChain> scc = new ArrayList<>();
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
    private void detectAffectedOCs(HashSet<OperationChain> affectedOCs, HashSet<OperationChain> circularOCs) {
        for (OperationChain circularOC : circularOCs) {
            dfs((OperationChain) circularOC, affectedOCs);
        }
    }

    public void dfs(OperationChain oc, HashSet<OperationChain> affectedOCs) {
        affectedOCs.add(oc);
        for (OperationChain childOC : oc.ocChildren.keySet()) {
            if (!affectedOCs.contains(childOC)) {
                dfs((OperationChain) childOC, affectedOCs);
            }
        }
    }

    private void detectAffectedOCs(HashSet<OperationChain> scannedOCs,
                                   HashSet<OperationChain> circularOCs,
                                   OperationChain oc) {
        if (oc.hasParents()) {
            if (circularOCs.contains(oc)) {
                return;
            }
            scannedOCs.clear();
            scannedOCs.add(oc);
            // scan from leaves and check whether circular are detected.
            if (oc.isCircularAffected(scannedOCs, circularOCs)) {
                circularOCs.add(oc);
                dfs(oc, circularOCs);
            }
        }
    }

    public OperationChain addOperationToChain(Operation operation, int targetThreadId) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        OperationChain retOc = getOC(table_name, primaryKey, targetThreadId);
        retOc.addOperation(operation);
        return retOc;
    }

    private OperationChain getOC(String tableName, String pKey, int threadId) {
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
//        return holder.get(pKey);
    }

    private OperationChain getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey, 0));
//        return holder.get(pKey);
    }

    private void checkFD(OperationChain curOC, Operation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;// no need to check data dependency on a key itself.
                OperationChain OCFromConditionSource = getOC(condition_sourceTable[index], condition_source[index]);
                // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
                MyList<Operation> conditionedOps = OCFromConditionSource.getOperations();
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

    public int getApp() {
        return app;
    }
}
