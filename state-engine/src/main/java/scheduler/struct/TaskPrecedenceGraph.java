package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.LayeredTPGContext;
import scheduler.context.SchedulerContext;
import transaction.impl.ordered.MyList;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;

import static scheduler.impl.Scheduler.getTaskId;

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
public class TaskPrecedenceGraph<Context extends SchedulerContext, ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain> {
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final ConcurrentHashMap<String, TableOCs<SchedulingUnit>> operationChains;//shared data structure.
    CyclicBarrier barrier;

    /**
     * @param totalThreads
     * @param delta
     */
    public TaskPrecedenceGraph(int totalThreads, int delta) {
        barrier = new CyclicBarrier(totalThreads);
        this.delta = delta;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs<SchedulingUnit>(totalThreads));
        operationChains.put("bookEntries", new TableOCs<SchedulingUnit>(totalThreads));
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
     * @param request
     */
    public void setupOperationTDFD(AbstractOperation operation, Request request, Context context) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation, context);
        // FD
        checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source, context);
    }

    public void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
            submit(context, tableOCs.threadOCsMap.get(threadId).holder_v1.values());
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
//        LOG.trace("++++++ end explore");
    }

    private void submit(Context context, Collection<SchedulingUnit> ocs) {
        if (context instanceof LayeredTPGContext) {
            for (SchedulingUnit oc : ocs) {
                ((LayeredTPGContext) context).totalOsToSchedule += oc.getOperations().size();
            }
            HashMap<Integer, ArrayList<SchedulingUnit>> layeredOCBucketPerThread = ((LayeredTPGContext) context).allocatedLayeredOCBucket;
            ((LayeredTPGContext) context).maxLevel = buildBucketPerThread(layeredOCBucketPerThread, ocs);
            System.out.println(((LayeredTPGContext) context).maxLevel);
        }
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, ArrayList<SchedulingUnit>> OCBucketThread,
                                    Collection<SchedulingUnit> ocs) {
        int localMaxDLevel = 0;
        for (SchedulingUnit oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayList<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
//        if(enable_log) LOG.debug("localMaxDLevel" + localMaxDLevel);
        return localMaxDLevel;
    }

    /**
     * @param operation
     */
    private SchedulingUnit addOperationToChain(AbstractOperation operation, Context context) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        SchedulingUnit retOc = getOC(table_name, primaryKey, context);
        retOc.addOperation(operation);
        return retOc;
    }


    private SchedulingUnit getOC(String tableName, String pKey, Context context) {
        ConcurrentHashMap<String, SchedulingUnit> holder = getTableOCs(tableName).threadOCsMap.get(getTaskId(pKey, delta)).holder_v1;
        return holder.computeIfAbsent(pKey, s -> (SchedulingUnit) context.createTask(tableName, pKey));
    }

    private void checkFD(SchedulingUnit curOC, AbstractOperation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source, Context context) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            SchedulingUnit OCFromConditionSource = getOC(condition_sourceTable[index], condition_source[index], context);
            // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
            MyList<AbstractOperation> conditionedOps = OCFromConditionSource.getOperations();
            if (OCFromConditionSource.getOperations().isEmpty() || conditionedOps.first().bid >= op.bid) {
                OCFromConditionSource.addPotentialFDChildren(curOC, op);
            } else {
                // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                curOC.addFDParent(op, OCFromConditionSource); // record dependency
            }
        }
        curOC.checkPotentialFDChildrenOnNewArrival(op);
    }
}
