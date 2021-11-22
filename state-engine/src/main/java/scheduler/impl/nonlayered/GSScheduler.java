package scheduler.impl.nonlayered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContext;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

import static common.CONTROL.enable_log;

public class GSScheduler extends AbstractGSScheduler<GSTPGContext, GSOperation, GSOperationChain> {
    private static final Logger log = LoggerFactory.getLogger(GSScheduler.class);

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public boolean needAbortHandling = false;

    public GSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(GSTPGContext context) {
//        tpg.constructTPG(context);
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }

    public void REINITIALIZE(GSTPGContext context) {
        tpg.secondTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }

    @Override
    public void start_evaluation(GSTPGContext context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;
//        System.out.println(threadId + " first explore tpg");

        INITIALIZE(context);
//        System.out.println(threadId + " first explore tpg complete, start to process");

        do {
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            EXPLORE(context);
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            PROCESS(context, mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        } while (!FINISHED(context));
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
        if (needAbortHandling) {
            if (enable_log) {
                log.info("need abort handling, rollback and redo");
            }
            // identify all aborted operations and transit the state to aborted.
            REINITIALIZE(context);
            // rollback to the starting point and redo.
            do {
                MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
                EXPLORE(context);
//                MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
                MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
                PROCESS(context, mark_ID);
                MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
                MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            } while (!FINISHED(context));
        }
        RESET(context);//
        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
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
    public void EXPLORE(GSTPGContext context) {
         context.partitionStateManager.handleStateTransitions();
    }

    @Override
    protected void NOTIFY(GSOperationChain task, GSTPGContext context) {
        context.partitionStateManager.onOcExecuted(task);
    }

    @Override
    protected void checkTransactionAbort(GSOperation operation, GSOperationChain operationChain) {
        // in coarse-grained algorithms, we will not handle transaction abort gracefully, just update the state of the operation
        operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
        // save the abort information and redo the batch.
        needAbortHandling = true;
    }


    @Override
    public void TxnSubmitFinished(GSTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<GSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            constructOp(operationGraph, request, context);
        }
        // set logical dependencies among all operation in the same transaction
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private void constructOp(List<GSOperation> operationGraph, Request request, GSTPGContext context) {
        long bid = request.txn_context.getBID();
        GSOperation set_op;
        GSTPGContext targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new GSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.function, request.condition, request.condition_records, request.success);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new GSOperation(request.src_key, targetContext, request.table_name, request.txn_context, bid, request.accessType,
                        request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                break;
            default:
                throw new RuntimeException("Unexpected operation");
        }
//        set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
        operationGraph.add(set_op);
//        set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
//        tpg.cacheToSortedOperations(set_op);
        tpg.setupOperationTDFD(set_op, request, targetContext);
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(GSOperationChain operationChain) {
            DISTRIBUTE(operationChain, (GSTPGContext) operationChain.context);//TODO: make it clear..
        }

        public void onOCFinalized(GSOperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
//            operationChain.context.operationChains.remove(operationChain);
        }

        public void onOCRollbacked(GSOperationChain operationChain) {
            operationChain.context.scheduledOPs -= operationChain.getOperations().size();
        }
    }
}
