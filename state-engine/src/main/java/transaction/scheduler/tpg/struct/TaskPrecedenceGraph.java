package transaction.scheduler.tpg.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    // all parameters in this class should be thread safe.
    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);
    private final ConcurrentHashMap<String, OperationChain> operationChains;// < state, OC>
    private final ConcurrentLinkedQueue<List<Operation>> transactions;//
    private final Queue<Operation> readyQueue; // short cut data structure for dependency exploration
    private final Queue<Operation> speculativeQueue; // short cut data structure for dependency exploration
    private final ShortCutListener shortCutListener;
    public static final AtomicInteger nPendingOperation = new AtomicInteger(0);
    private final AtomicInteger nExecutedOperation = new AtomicInteger(0);
    private TPGScheduler.ExecutableTaskListener executableTaskListener = null;

    public final static int Maximum_Speculation = 10;
    private int totalThreads;
    CyclicBarrier barrier;

    /**
     * @param totalThreads
     */
    public TaskPrecedenceGraph(int totalThreads) {
//        this.threadmapping = threadmappinging;
        this.totalThreads = totalThreads;
        barrier = new CyclicBarrier(totalThreads);
        operationChains = new ConcurrentHashMap<>();
        transactions = new ConcurrentLinkedQueue<>();
        readyQueue = new ConcurrentLinkedQueue<>();
        speculativeQueue = new ConcurrentLinkedQueue<>();
        shortCutListener = new ShortCutListener();
    }

    /**
     * Add operations of transactions to TPG, which tries to find out the temporal dependencies
     *
     * @param operations
     */
    public void addTxn(List<Operation> operations) {
        // add Logical dependnecies for those operations in the operation graph
        // two operations can have both data dependency and logical dependency.
        transactions.add(operations);
        Operation headerOperation = operations.get(0);
        headerOperation.setReadyCandidate();
        for (int i = 0; i < operations.size(); i++) {
            Operation curOperation = operations.get(i);
            curOperation.setExecutableOperationListener(shortCutListener);
            nPendingOperation.incrementAndGet();
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
                if (i - reversedOffset >= 0) // TODO: we should sort the queue to ensure the order and make sure the speculative operation can be identified.
                    curOperation.addParent(operations.get(i - reversedOffset), MetaTypes.DependencyType.SP_LD);
//                if (i+offset < operations.size() && offset != Maximum_Speculation) // because speculation is emit from ready operation, it should eliminate itself for parallel execute
                if (i + offset < operations.size())
                    curOperation.addChild(operations.get(i + offset), MetaTypes.DependencyType.SP_LD);
            }
            addOperationToChain(curOperation);
        }
//        LOG.debug("++++++operations: " + operations);
    }

    /**
     * get operations from readyQueue and speculativeQueue.
     *
     * @return
     */
    public Operation exploreReady() {
        LOG.trace("readyQueue size: " + readyQueue.size());
        return readyQueue.poll();
    }

    /**
     * get operations from readyQueue and speculativeQueue.
     *
     * @return
     */
    public Operation exploreSpeculative() {
        LOG.trace("speculativeQueue size: " + speculativeQueue.size());
        return speculativeQueue.poll();
    }

    /**
     * @param threadId
     */
    public void firstTimeExploreTPG(int threadId) {
        Controller.stateManagers.get(threadId).initialize(shortCutListener);
        for (String key : Controller.threadtoStateMapping.get(threadId)) {
            operationChains.computeIfPresent(key, (s, operationChain) -> {
                operationChain.updateTDDependencies();
                Operation head = operationChain.getOperations().first();
//                head.exploreReadyOperation();
                if (head.isRoot()) {
                    Controller.stateManagers.get(threadId).onRootStart(head);
                }
                return operationChain;
            });
        }
        LOG.trace("++++++ end explore");
    }

    /**
     * @param operation
     */
    private void addOperationToChain(Operation operation) {
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        String table_name = operation.table_name;
        String primaryKey = operation.d_record.record_.GetPrimaryKey();
        String operationChainKey = operation.getOperationChainKey();
        OperationChain retOc = operationChains.computeIfAbsent(operationChainKey, s -> new OperationChain(table_name, primaryKey));
        retOc.addOperation(operation);
    }

    public void setExecutableListener(TPGScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }

    /**
     * Register an operation to queue.
     */
    public class ShortCutListener {
        public void onExecutable(Operation operation, boolean isReady) {
            if (isReady) {
                readyQueue.add(operation);
            } else {
                speculativeQueue.add(operation);
            }
        }

        public void onExecutable(Operation operation, boolean isReady, int threadId) {
            executableTaskListener.onExecutable(operation, threadId);
        }

        public void onOperationFinalized(Operation operation, boolean isCommitted) {
            LOG.debug("npending: " + nPendingOperation.get());
            nPendingOperation.decrementAndGet();
        }

        public void onOperationExecuted() {
            LOG.debug("nexecuted: " + nExecutedOperation.get());
            nExecutedOperation.incrementAndGet();
        }
    }

    /**
     * expose an api to check whether all operations are in the final state i.e. aborted/committed
     */
    public boolean isFinished() {
        LOG.debug("operations left to do:" + nPendingOperation.get());
        return nPendingOperation.get() == 0;
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
}
