package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.LayeredTPGContext;
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
public class TaskPrecedenceGraph {
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    //    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentHashMap<String, OperationChain>>> operationChains;// table -> taskid -> < state, OC>
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
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
        operationChains.put("accounts", new TableOCs(totalThreads));
        operationChains.put("bookEntries", new TableOCs(totalThreads));
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
        checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
    }

    public <Context extends LayeredTPGContext> void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        Collection<TableOCs> tableOCsList = getOperationChains().values();
        for (TableOCs tableOCs : tableOCsList) {//for each table.
            submit(context, tableOCs.threadOCsMap.get(threadId).holder_v1.values());
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);

//        LOG.trace("++++++ end explore");
    }

    private <Context extends LayeredTPGContext> void submit(Context context, Collection<OperationChain> ocs) {
        for (OperationChain oc : ocs) {
            context.totalOsToSchedule += oc.getOperations().size();
        }
        HashMap<Integer, ArrayList<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal;
        context.maxLevel = buildBucketPerThread(layeredOCBucketThread, ocs);
        System.out.println(context.maxLevel);
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, ArrayList<OperationChain>> OCBucketThread,
                                    Collection<OperationChain> ocs) {
        int localMaxDLevel = 0;
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayList<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
//        LOG.debug("localMaxDLevel" + localMaxDLevel);
        return localMaxDLevel;
    }

    /**
     * @param operation
     */
    private OperationChain addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
//        String operationChainKey = operation.getOperationChainKey();
        OperationChain retOc = getOC(table_name, primaryKey);
        retOc.addOperation(operation);
        return retOc;
    }

//    @NotNull
//    private OperationChain getOC(String table_name, String primaryKey, String operationChainKey) {
//        return operationChains.get(getTaskId(primaryKey, delta)).computeIfAbsent(operationChainKey, s -> {
//            nPendingOCs.incrementAndGet();
//            return new OperationChain(table_name, primaryKey);
//        });
//    }

    private OperationChain getOC(String tableName, String pKey) {
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(getTaskId(pKey, delta)).holder_v1;
        return holder.computeIfAbsent(pKey, s -> new OperationChain(tableName, pKey));
    }

    private void checkFD(OperationChain curOC, Operation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
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
