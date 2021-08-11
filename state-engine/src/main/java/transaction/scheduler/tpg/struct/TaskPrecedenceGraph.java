package transaction.scheduler.tpg.struct;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.scheduler.Request;
import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.TPGScheduler;

import java.util.List;
import java.util.Queue;
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
    private final ShortCutListener shortCutListener;
    private final AtomicInteger nExecutedOperation = new AtomicInteger(0);
    CyclicBarrier barrier;
    private TPGScheduler.ExecutableTaskListener executableTaskListener = null;

    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads) {
        barrier = new CyclicBarrier(totalThreads);
        operationChains = new ConcurrentHashMap<>();
        transactions = new ConcurrentLinkedQueue<>();
        shortCutListener = new ShortCutListener();
    }

    public void setupOperationChain(Operation operation, Request request) {
        OperationChain oc = addOperationToChain(operation);
        checkDataDependencies(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
    }

    /**
     * Add operations of transactions to TPG, which tries to find out the temporal dependencies
     *
     * @param operations
     */
    public void setupOperations(List<Operation> operations) {
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

            // add header for transaction commit and abort
            curOperation.addHeader(headerOperation);
            // add all descendants to the descendant operation list
            headerOperation.addDescendant(curOperation);

            // add speculative parents/children for speculative parallelization
            // ld_spec_children/parents include the direct children/parents for convenience
            for (int offset = 1; offset < Maximum_Speculation + 1; offset++) {
                int reversedOffset = (Maximum_Speculation + 1 - offset);
                if (i - reversedOffset >= 0)
                    curOperation.addParent(operations.get(i - reversedOffset), MetaTypes.DependencyType.SP_LD);
//                if (i+offset < operations.size() && offset != Maximum_Speculation) // because speculation is emit from ready operation, it should eliminate itself for parallel execute
                if (i + offset < operations.size())
                    curOperation.addChild(operations.get(i + offset), MetaTypes.DependencyType.SP_LD);
            }
        }
    }


    public <Context extends TPGContext> void firstTimeExploreTPG(Context context) {
        context.initialize(shortCutListener);
        for (String key : context.partitionStateManager.partition) {
            operationChains.computeIfPresent(key, (s, operationChain) -> {
                // update functional dependencies and logical dependencies
                if (!operationChain.hasParents()) {
                    operationChain.context.partitionStateManager.onOcRootStart(operationChain);
                }
                return operationChain;
            });
        }
        LOG.trace("++++++ end explore");
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

    private void checkDataDependencies(OperationChain curOC, Operation op, String table_name,
                                       String key, String[] condition_sourceTable, String[] condition_source) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            String operationChainKey = condition_sourceTable[index] + "|" + condition_source[index];
            OperationChain OCFromConditionSource = getOC(condition_sourceTable[index],
                    condition_source[index], operationChainKey);
            // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
            if (OCFromConditionSource.getOperations().isEmpty() || OCFromConditionSource.getOperations().first().bid >= op.bid) {
                OCFromConditionSource.addPotentialChildren(curOC, op);
            } else {
                // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                curOC.addParent(op, OCFromConditionSource); // record dependency
            }
        }
        curOC.checkPotentialChildrenOnNewArrival(op);
    }

    public void setExecutableListener(TPGScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }

    /**
     * expose an api to check whether all operations are in the final state i.e. aborted/committed
     */
    public boolean isFinished() {
        LOG.trace("operations left to do:" + nPendingOCs.get());
        return nPendingOCs.get() == 0;
    }

    /**
     * @param threadId
     */
    public void dumpOCState(int threadId) {
        if (threadId == 0) {
            LOG.info("================Operation Chain=================");
            for (OperationChain operationChain : operationChains.values()) {
                StringBuilder output = new StringBuilder();
                output.append(operationChain.getTableName()).append(operationChain.getPrimaryKey()).append("==");
                for (Operation curOp : operationChain.getOperations()) {
                    output.append(curOp).append(": ").append(curOp.getOperationState()).append(",");
                }
                LOG.info(String.valueOf(output));
            }
        }
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param threadId
     */
    public void dumpTxnState(int threadId) {
        if (threadId == 0) {
            LOG.info("=================Transactions================");
            for (List<Operation> operations : transactions) {
                LOG.info(String.valueOf(operations));
            }
        }
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param threadId
     */
    public void dumpStatusByDependency(int threadId) {
        if (threadId == 0) {
            LOG.info("=================Transactions================");
            for (List<Operation> operations : transactions) {
                for (Operation operation : operations) {
                    StringBuilder output = new StringBuilder();
                    output.append(dumpChildren(operation)).append(" => ");
                    LOG.info(String.valueOf(output));
                }
            }
        }
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public StringBuilder dumpChildren(Operation operation) {
        StringBuilder output = new StringBuilder();
        output.append(operation).append(": ");
        output.append("[");
        Queue<Operation> td_children = operation.getChildren(MetaTypes.DependencyType.TD);
        output.append("TD: ").append(td_children);
        output.append(", ");

        Queue<Operation> fd_children = operation.getChildren(MetaTypes.DependencyType.FD);
        output.append("FD: ").append(fd_children);
        output.append(", ");

        Queue<Operation> ld_children = operation.getChildren(MetaTypes.DependencyType.LD);
        output.append("LD: ").append(ld_children);
        output.append("]");
        return output;
    }

    /**
     * @param threadId
     * @return
     */
    public boolean isValid(int threadId) {
        for (List<Operation> operations : transactions) {
            if (operations.get(0).getOperationState().equals(MetaTypes.OperationStateType.COMMITTED)) {
                // transactions operations are all committed
                for (Operation operation : operations) {
                    if (!(operation.getOperationState().equals(MetaTypes.OperationStateType.COMMITTED) || operation.getOperationState().equals(MetaTypes.OperationStateType.COMMITTABLE))) {
                        LOG.info("++++++Wrong transaction committed state, not all operations are committed: " + operation + " : " + operations);
                        return false;
                    }
                }
            } else {
                // transactions operations are in non-committed state
                for (Operation operation : operations) {
                    if (operation.getOperationState().equals(MetaTypes.OperationStateType.COMMITTED)) {
                        LOG.info("++++++Wrong transaction non-committed state, exists operations committed: " + operations);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Register an operation to queue.
     */
    public class ShortCutListener {
        public void onExecutable(OperationChain operationChain) {
            executableTaskListener.onExecutable(operationChain);
        }

        public void onOperationFinalized(Operation operation, boolean isCommitted) {
            LOG.info("npending: " + nPendingOCs.get());
            nPendingOCs.decrementAndGet();
        }

        public void onOCFinalized() {
            LOG.debug("npending: " + nPendingOCs.get());
            nPendingOCs.decrementAndGet();
        }

        public void onOperationExecuted() {
            LOG.debug("nexecuted: " + nExecutedOperation.get());
            nExecutedOperation.incrementAndGet();
        }
    }
}
