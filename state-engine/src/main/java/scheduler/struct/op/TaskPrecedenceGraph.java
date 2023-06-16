package scheduler.struct.op;

import content.common.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.op.OPNSContext;
import scheduler.context.op.OPSContext;
import scheduler.context.op.OPSchedulerContext;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Vector;
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
public class TaskPrecedenceGraph<Context extends OPSchedulerContext> {
    private static final Logger log = LoggerFactory.getLogger(TaskPrecedenceGraph.class);

    public final int totalThreads;
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    public final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final int app;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    private final Vector<Operation> NonOperations = new Vector<>();
    CyclicBarrier barrier;
    private int maxLevel = 0; // just for layered scheduling


    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS, int app) {
        barrier = new CyclicBarrier(totalThreads);
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        // all parameters in this class should be thread safe.
        threadToContextMap = new ConcurrentHashMap<>();
        threadToOCs = new ConcurrentHashMap<>();
        this.app = app;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
    }

    public void reset(Context context) {
//        if (app == 0) {
//            operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 1) {
//            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 2) {
//            operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//            operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        } else if (app == 3) {
//            operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
//        }
//        threadToOCs.get(context.thisThreadId).clear();
//        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
//            oc.clear();
//        }
//        this.setOCs(context); // TODO: the short cut should be reset, but will take some time.
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            oc.clear(); // only need to clear all operations from all ocs
        }
        NonOperations.clear();
        if (context.thisThreadId == 0) log.info("===Clear current data for the next batch===");
    }

    public void initTPG(int offset) {
        if (app == 0 || app == 5) {//GS
            operationChains.put("MicroTable", new TableOCs(totalThreads, offset));
        } else if (app == 1) {//SL
            operationChains.put("accounts", new TableOCs(totalThreads, offset));
            operationChains.put("bookEntries", new TableOCs(totalThreads, offset));
        } else if (app == 2) {//TP
            operationChains.put("segment_speed", new TableOCs(totalThreads, offset));
            operationChains.put("segment_cnt", new TableOCs(totalThreads, offset));
        } else if (app == 3) {//OB
            operationChains.put("goods", new TableOCs(totalThreads, offset));
        } else if (app == 4) {//ED
            //TODO: Put ED tables into operation chain
            operationChains.put("word_table", new TableOCs(totalThreads, offset));
            operationChains.put("tweet_table", new TableOCs(totalThreads, offset));
            operationChains.put("cluster_table", new TableOCs(totalThreads, offset));
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
            if (app == 0 || app == 5) {
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
                OperationChain speedOC = context.createTask("segment_speed", _key);
                OperationChain cntOC = context.createTask("segment_cnt", _key);
                operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, speedOC);
                operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, cntOC);
                ocs.add(speedOC);
                ocs.add(cntOC);
            } else if (app == 3) {
                OperationChain gsOC = context.createTask("goods", _key);
                operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, gsOC);
                ocs.add(gsOC);
            } else if (app == 4) {
                //TODO: Add ED here
                OperationChain wordOC = context.createTask("word_table", _key);
                OperationChain tweetOC = context.createTask("tweet_table", _key);
                OperationChain clusterOC = context.createTask("cluster_table", _key);
                operationChains.get("word_table").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, wordOC);
                operationChains.get("tweet_table").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, tweetOC);
                operationChains.get("cluster_table").threadOCsMap.get(context.thisThreadId).holder_v1.put(_key, clusterOC);
                ocs.add(wordOC);
                ocs.add(tweetOC);
                ocs.add(clusterOC);
            }
        }
        threadToOCs.put(context.thisThreadId, ocs);
    }

    private void resetOCs(Context context) {
        if (app == 0 || app == 5) {
            operationChains.get("MicroTable").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 1) {
            operationChains.get("accounts").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("bookEntries").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 2) {
            operationChains.get("segment_speed").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("segment_cnt").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 3) {
            operationChains.get("goods").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
        } else if (app == 4) {
            //TODO: Add ED tables here
            operationChains.get("word_table").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("tweet_table").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
            operationChains.get("cluster_table").threadOCsMap.get(context.thisThreadId).holder_v1.clear();
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
     * set up functional dependencies among operations
     *
     * @param operation
     * @param request
     */
    public void setupOperationTDFD(Operation operation, Request request) {
        // TD
        OperationChain oc = addOperationToChain(operation);
        if (operation.isNonDeterministicOperation) {
            checkDependencyForNonDeterministicStateAccess(oc, operation);
        } else {
            // FD
            if (request.condition_source != null)
                checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
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
                    oc.addNonOperation(this.NonOperations);
                    oc.updateDependencies();
                }
            }
            context.waitForOtherThreads(context.thisThreadId);
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.initializeDependencies();
                    Operation head = oc.getOperations().first();
                    if (head.isRoot()) {
                        roots.add(head);
                    }
                    context.operations.addAll(oc.getOperations());
                    context.totalOsToSchedule += oc.getOperations().size();
                }
            }
            context.waitForOtherThreads(context.thisThreadId);
            ((OPSContext) context).buildBucketPerThread(context.operations, roots);
            context.waitForOtherThreads(context.thisThreadId);
            if (context.thisThreadId == 0) { // gather
                for (Context curContext : threadToContextMap.values()) {
                    if (((OPSContext) curContext).maxLevel > maxLevel) {
                        maxLevel = ((OPSContext) curContext).maxLevel;
                    }
                }
            }
            context.waitForOtherThreads(context.thisThreadId);
            ((OPSContext) context).maxLevel = maxLevel; // scatter
            if (enable_log) log.info("MaxLevel:" + (((OPSContext) context).maxLevel));
        } else if (context instanceof OPNSContext) {
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.updateDependencies();
                }
            }
            context.waitForOtherThreads(context.thisThreadId);
            for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
                if (!oc.getOperations().isEmpty()) {
                    oc.initializeDependencies();
                    Operation head = oc.getOperations().first();
                    context.totalOsToSchedule += oc.getOperations().size();
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

    public void secondTimeExploreTPG(Context context) {
        context.redo();
        for (OperationChain oc : threadToOCs.get(context.thisThreadId)) {
            if (!oc.getOperations().isEmpty()) {
                resetOp(oc);
            }
        }
        context.waitForOtherThreads(context.thisThreadId);
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
                OCFromConditionSource.addPotentialFDChildren(curOC, op);
            }
        }
    }
    private void checkDependencyForNonDeterministicStateAccess(OperationChain curOC, Operation op) {
        //Add Non-deterministic state access operation to all its potential parents
        NonOperations.add(op);
    }

    public int getApp() {
        return app;
    }
}
