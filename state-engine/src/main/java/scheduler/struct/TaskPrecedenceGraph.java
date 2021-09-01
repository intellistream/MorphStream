package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.AbstractGSTPGContext;
import scheduler.context.LayeredTPGContext;
import scheduler.context.SchedulerContext;
import scheduler.struct.gs.AbstractGSOperationChain;
import transaction.impl.ordered.MyList;
import utils.lib.ConcurrentHashMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static common.CONTROL.enable_log;
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
public class TaskPrecedenceGraph<Context extends SchedulerContext<SchedulingUnit>, SchedulingUnit extends OperationChain<ExecutionUnit>, ExecutionUnit extends AbstractOperation> {
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final Map<Integer, Context> threadToContextMap;
    private final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private ConcurrentHashMap<String, TableOCs<SchedulingUnit>> operationChains;//shared data structure.
    CyclicBarrier barrier;

    public void reset() {
        //reset holder.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs<>(totalThreads));
        operationChains.put("bookEntries", new TableOCs<>(totalThreads));
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
    public SchedulingUnit setupOperationTDFD(ExecutionUnit operation, Request request) {
        // TD
        SchedulingUnit oc = addOperationToChain(operation);
        // FD
        checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
        return oc;
    }

    public void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        Collection<TableOCs<SchedulingUnit>> tableOCsList = getOperationChains().values();
        for (TableOCs<SchedulingUnit> tableOCs : tableOCsList) {//for each table.
            submit(context, tableOCs.threadOCsMap.get(threadId).holder_v1.values());
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private void submit(Context context, Collection<SchedulingUnit> ocs) {
        if (context instanceof LayeredTPGContext) {
            for (SchedulingUnit oc : ocs) {
                context.totalOsToSchedule += oc.getOperations().size();
            }
            ((LayeredTPGContext) context).buildBucketPerThread(ocs);
            if (enable_log) LOG.info("MaxLevel:" + (((LayeredTPGContext) context).maxLevel));
        } else if (context instanceof AbstractGSTPGContext) {
            for (SchedulingUnit oc : ocs) {
                context.totalOsToSchedule += oc.getOperations().size();
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
    }

    /**
     * @param operation
     */
    private SchedulingUnit addOperationToChain(ExecutionUnit operation) {
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
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
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
                curOC.addFDParent(op, OCFromConditionSource); // record dependency
            }
        }
        curOC.checkPotentialFDChildrenOnNewArrival(op);
    }

}
