package transaction.scheduler.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.TableRecord;
import transaction.dedicated.ordered.MyList;
import transaction.scheduler.Request;
import transaction.scheduler.Scheduler;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;
import utils.SOURCE_CONTROL;

import java.util.*;

import static common.meta.CommonMetaTypes.AccessType.*;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
//@lombok.extern.slf4j.Slf4j
public class TPGScheduler<Context extends LayeredTPGContext> extends Scheduler<Context, OperationChain> {
    private static final Logger log = LoggerFactory.getLogger(TPGScheduler.class);
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph tpg; // TPG to be maintained in this global instance.
    public final Map<Integer, Context> threadToContextMap;

    public TPGScheduler(int totalThreads, int NUM_ITEMS) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph(totalThreads, delta);
        threadToContextMap = new HashMap<>();
    }

    @Override
    public void INITIALIZE(Context context) {
        int threadId = context.thisThreadId;
        tpg.firstTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected OperationChain BFSearch(Context context) {
        ArrayList<OperationChain> ocs = context.BFSearch(); //
        OperationChain oc = null;
        if (ocs != null && context.currentLevelIndex < ocs.size()) {
            oc = ocs.get(context.currentLevelIndex++);
            context.scheduledOPs += oc.getOperations().size();
        }
        return oc;
    }

    private void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(Context context) {
        OperationChain next = BFSearch(context);
        if (next == null && !context.finished()) {//current level is all processed at the current thread.
            //Ready to proceed to next level
            //Check if there's any aborts
            while (next == null) {
                ProcessedToNextLevel(context);
                next = BFSearch(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
            }
        }
        DISTRIBUTE(next, context);
    }

    @Override
    public boolean FINISHED(Context context) {
//        return tpg.isFinished();
        return context.finished();

    }

    @Override
    public void RESET() {
//        Controller.exec.shutdownNow();
        SOURCE_CONTROL.getInstance().oneThreadCompleted();
    }

    /**
     * Submit requests to target thread --> data shuffling is involved.
     *
     * @param context
     * @param request
     * @return
     */
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.requests.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public void AddContext(int threadId, Context context) {
        threadToContextMap.put(threadId, context);
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<Operation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op = null;
            switch (request.accessType) {
                case READ_WRITE_COND: // they can use the same method for processing
                case READ_WRITE:
                    set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new Operation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
            }
            operationGraph.add(set_op);
            tpg.setupOperationTDFD(set_op, request);
        }

        // 4. send operation graph to tpg for tpg construction
//        tpg.setupOperationLD(operationGraph);//TODO: this is bad refactor.
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey(), delta);
        return threadToContextMap.get(threadId);
    }

    /**
     * Used by tpgScheduler.
     *
     * @param thisThreadId
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(int thisThreadId, Operation operation, long mark_ID, boolean clean) {
//        log.debug("++++++execute: " + operation);

        // the operation will only be executed when the state is in READY/SPECULATIVE,
        int success = operation.success[0];
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            CT_Transfer_Fun(thisThreadId, operation, mark_ID, clean);
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            CT_Transfer_Fun(thisThreadId, operation, mark_ID, clean);
        } else if (operation.accessType.equals(READ_WRITE)) {
            CT_Depo_Fun(operation, mark_ID, clean);
        } else {
            throw new UnsupportedOperationException();
        }
        // operation success check, number of operation succeeded does not increase after execution
        if (operation.success[0] == success) {
            operation.isFailed = true;
        }
//        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;

    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        OperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            execute(context, next.getOperations(), mark_ID);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
//            log.debug("finished execute current operation chain: " + next.toString());
        }
    }

    /**
     * Used by BFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<Operation> operation_chain, long mark_ID) {
        Operation operation = operation_chain.pollFirst();
        while (operation != null) {
            Operation finalOperation = operation;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(context.thisThreadId, finalOperation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);

            operation = operation_chain.pollFirst();
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private OperationChain next(Context context) {
        OperationChain operationChain = context.ready_oc;
        context.ready_oc = null;
        return operationChain;// if a null is returned, it means, we are done with this level!
    }


    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     *
     */
    @Override
    protected void DISTRIBUTE(OperationChain task, Context context) {
        context.ready_oc = task;
    }


}
