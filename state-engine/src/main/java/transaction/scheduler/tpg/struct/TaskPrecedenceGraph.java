package transaction.scheduler.tpg.struct;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.scheduler.Request;
import transaction.scheduler.tpg.LayeredTPGContext;
import transaction.scheduler.tpg.TPGContext;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

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
    public final AtomicInteger nPendingOCs = new AtomicInteger(0);
    public final static int Maximum_Speculation = 10;
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);
    private final ConcurrentHashMap<String, OperationChain> operationChains;// < state, OC>
    private final ConcurrentLinkedQueue<List<Operation>> transactions;//
    private final AtomicInteger nExecutedOperation = new AtomicInteger(0);
    CyclicBarrier barrier;

    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads) {
        barrier = new CyclicBarrier(totalThreads);
        operationChains = new ConcurrentHashMap<>();
        transactions = new ConcurrentLinkedQueue<>();
    }

    /**
     * set up functional dependencies among operations
     * @param operation
     * @param request
     */
    public void setupOperationTDFD(Operation operation, Request request) {
        // TD
        OperationChain oc = addOperationToChain(operation);
        // FD
        checkFD(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
    }

    /**
     * Add operations of transactions to TPG, which tries to find out the temporal dependencies
     * Set up logical dependencies among operations
     * @param operations
     */
    public void setupOperationLD(List<Operation> operations) {
        // LD
        // add Logical dependnecies for those operations in the operation graph
        // two operations can have both data dependency and logical dependency.
        transactions.add(operations);
        Operation headerOperation = operations.get(0);
        headerOperation.setReadyCandidate();
        for (int i = 0; i < operations.size(); i++) {
            Operation curOperation = operations.get(i);
            if (i > 0)
                curOperation.addParent(operations.get(i - 1), MetaTypes.DependencyType.LD);
            if (i < operations.size() - 1)
                curOperation.addChild(operations.get(i + 1), MetaTypes.DependencyType.LD);

            // add an operation id for the operation for the purpose of temporal dependency construction
            curOperation.set_op_id(i);
        }
    }


    public <Context extends LayeredTPGContext> void firstTimeExploreTPG(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        Collection<OperationChain> ocs = new ArrayList<>();
        // construct layered oc for bfs search or dfs search, TODO: need to be optimized
        for (String key : context.partitionStateManager.partition) {
            if (operationChains.containsKey(key)) {
                ocs.add(operationChains.get(key));
            }
        }
        submit(context, ocs);
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);

        LOG.trace("++++++ end explore");
    }

    private <Context extends LayeredTPGContext> void submit(Context context, Collection<OperationChain> ocs) {
        for (OperationChain oc : ocs) {
            context.totalOsToSchedule += oc.getOperations().size();
        }
        HashMap<Integer, ArrayList<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal;
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
        LOG.debug("localMaxDLevel" + localMaxDLevel);
        return localMaxDLevel;
    }

    /**
     * @param operation
     */
    private OperationChain addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        String operationChainKey = operation.getOperationChainKey();
        OperationChain retOc = getOC(table_name, primaryKey, operationChainKey);
        retOc.addOperation(operation);
        return retOc;
    }

    @NotNull
    private OperationChain getOC(String table_name, String primaryKey, String operationChainKey) {
        return operationChains.computeIfAbsent(operationChainKey, s -> {
            nPendingOCs.incrementAndGet();
            return new OperationChain(table_name, primaryKey);
        });
    }

    private void checkFD(OperationChain curOC, Operation op, String table_name,
                         String key, String[] condition_sourceTable, String[] condition_source) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            String operationChainKey = condition_sourceTable[index] + "|" + condition_source[index];
            OperationChain OCFromConditionSource = getOC(condition_sourceTable[index],
                    condition_source[index], operationChainKey);
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

    /**
     * expose an api to check whether all operations are in the final state i.e. aborted/committed
     */
    public boolean isFinished() {
        LOG.trace("operations left to do:" + nPendingOCs.get());
        return nPendingOCs.get() == 0;
    }

}
