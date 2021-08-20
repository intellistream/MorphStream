package scheduler.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.LayeredTPGContext;
import scheduler.struct.dfs.DFSOperationChain;
import transaction.impl.ordered.MyList;
import utils.lib.ConcurrentHashMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Supplier;

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
public class TaskPrecedenceGraph<OC extends OperationChain> {
    Supplier<OC> supplier;
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    CyclicBarrier barrier;

    public TaskPrecedenceGraph(int totalThreads, int delta, Supplier<OC> supplier) {
        barrier = new CyclicBarrier(totalThreads);
        this.delta = delta;
        //create holder.
        operationChains = new ConcurrentHashMap<>();
        operationChains.put("accounts", new TableOCs<OC>(totalThreads));
        operationChains.put("bookEntries", new TableOCs<OC>(totalThreads));
        this.supplier = supplier;
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
    public void setupOperationTDFD(AbstractOperation operation, Request request) {
        // TD
        OC oc = addOperationToChain(operation);
        // FD
        checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
    }

    public <Context extends LayeredTPGContext> void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        Collection<TableOCs> tableOCsList = getOperationChains().values();
        for (TableOCs tableOCs : tableOCsList) {//for each table.
            ConcurrentHashMap<Integer, Holder> ocs = tableOCs.threadOCsMap;
            submit(context, ocs.get(threadId).holder_v1.values());
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
//        LOG.trace("++++++ end explore");
    }

    private <Context extends LayeredTPGContext> void submit(Context context, Collection<OC> ocs) {
        for (OC oc : ocs) {
            context.totalOsToSchedule += oc.getOperations().size();
        }
        HashMap<Integer, ArrayList<OC>> layeredOCBucketThread = context.layeredOCBucketGlobal;
        context.maxLevel = buildBucketPerThread(layeredOCBucketThread, ocs);
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, ArrayList<OC>> OCBucketThread,
                                    Collection<OC> ocs) {
        int localMaxDLevel = 0;
        for (OC oc : ocs) {
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
    private OC addOperationToChain(AbstractOperation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        OC retOc = getOC(table_name, primaryKey);
        retOc.addOperation(operation);
        return retOc;
    }

    private OC getOC(String tableName, String pKey) {
        ConcurrentHashMap<Integer, Holder> ocs = getTableOCs(tableName).threadOCsMap;
        ConcurrentHashMap<String, OC> holder = ocs.get(getTaskId(pKey, delta)).holder_v1;
        return holder.computeIfAbsent(pKey, s -> {
            OC oc = supplier.get();
            oc.initialize(tableName, pKey);
            return oc;
        });
//        return holder.computeIfAbsent(pKey, s -> new DFSOperationChain(tableName, pKey));
    }

    private void checkFD(OC curOC, AbstractOperation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            OC OCFromConditionSource = getOC(condition_sourceTable[index],
                    condition_source[index]);
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
