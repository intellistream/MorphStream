package scheduler.oplevel.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.LayeredTPGContext;
import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.context.OPLayeredContext;
import scheduler.oplevel.context.OPSchedulerContext;
import scheduler.oplevel.impl.tpg.OPBFSScheduler;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static common.CONTROL.enable_log;
import static scheduler.oplevel.impl.OPScheduler.getTaskId;

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

    protected final int delta;//range of each partition. depends on the number of op in the stage.
    CyclicBarrier barrier;
    public final Map<Integer, Context> threadToContextMap;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.

    public void reset(Context context) {
        operationChains.get("accounts").threadOCsMap.remove(context.thisThreadId);
        operationChains.get("bookEntries").threadOCsMap.remove(context.thisThreadId);
    }

    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads, int delta) {
        barrier = new CyclicBarrier(totalThreads);
        this.delta = delta;
        // all parameters in this class should be thread safe.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs(totalThreads));
        operationChains.put("bookEntries", new TableOCs(totalThreads));
        threadToContextMap = new HashMap<>();
    }

    public TableOCs getTableOCs(String table_name) {
        return operationChains.get(table_name);
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
        // FD
        if (request.condition_source != null)
            checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
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
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);

        if (context instanceof OPLayeredContext) {
            int threadId = context.thisThreadId;
            ArrayDeque<Operation> roots = new ArrayDeque<>();
            Collection<TableOCs> tableOCsList = getOperationChains().values();
            for (TableOCs tableOCs : tableOCsList) {//for each table.
                for (OperationChain oc : tableOCs.threadOCsMap.get(threadId).holder_v1.values()) {
                    oc.updateTDDependencies();
                    Operation head = oc.getOperations().first();
                    if (head.isRoot()) {
                        roots.add(head);
                    }
                    context.operations.addAll(oc.getOperations());
                    context.totalOsToSchedule += oc.getOperations().size();
                }
            }
            ((OPLayeredContext) context).buildBucketPerThread(context.operations, roots);
            if (enable_log) log.info("MaxLevel:" + (((OPLayeredContext) context).maxLevel));
        } else if (context instanceof OPGSTPGContext) {
            int threadId = context.thisThreadId;
            Collection<TableOCs> tableOCsList = getOperationChains().values();
            for (TableOCs tableOCs : tableOCsList) {//for each table.
                for (OperationChain oc : tableOCs.threadOCsMap.get(threadId).holder_v1.values()) {
                    oc.updateTDDependencies();
//                context.operations.addAll(oc.getOperations());
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

        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    /**
     * @param operation
     */
    private OperationChain addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        OperationChain retOc = getOC(table_name, primaryKey);
        retOc.addOperation(operation);
        return retOc;
    }

    private OperationChain getOC(String tableName, String pKey) {
        int threadId = getTaskId(pKey, delta);
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
    }

    private void checkFD(OperationChain curOC, Operation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;// no need to check data dependency on a key itself.
                OperationChain OCFromConditionSource = getOC(condition_sourceTable[index],
                        condition_source[index]);
                // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
                if (OCFromConditionSource.getOperations().isEmpty() || OCFromConditionSource.getOperations().first().bid >= op.bid) {
                    OCFromConditionSource.addPotentialFDChildren(curOC, op);
                } else {
                    // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                    curOC.addFDParent(op, OCFromConditionSource); // record dependency
                }
            }
            curOC.checkPotentialFDChildrenOnNewArrival(op);
        }
    }
}
