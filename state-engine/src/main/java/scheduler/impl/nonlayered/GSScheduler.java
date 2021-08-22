package scheduler.impl.nonlayered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContext;
import scheduler.impl.Scheduler;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;
import transaction.impl.ordered.MyList;

import java.util.ArrayList;
import java.util.List;

public class GSScheduler extends Scheduler<GSTPGContext, GSOperation, GSOperationChain> {

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public GSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(GSTPGContext context) {
        tpg.firstTimeExploreTPG(context);
        context.partitionStateManager.initialize(executableTaskListener);
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
    public boolean FINISHED(GSTPGContext context) {
        return context.finished();
    }

    @Override
    public void RESET() {
//        Controller.exec.shutdownNow();
    }

    @Override
    public void TxnSubmitFinished(GSTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<GSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            GSOperation set_op = null;
            switch (request.accessType) {
                case READ_WRITE_COND: // they can use the same method for processing
                case READ_WRITE:
                    set_op = new GSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new GSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
            }
            operationGraph.add(set_op);
            tpg.setupOperationTDFD(set_op, request, context);
        }
        // 4. send operation graph to tpg for tpg construction
//        tpg.setupOperationLD(operationGraph);//TODO: this is bad refactor.
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    @Override
    public void PROCESS(GSTPGContext context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
        GSOperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            for (GSOperation operation : next.getOperations()) {
                execute(operation, mark_ID, false);
            }
            NOTIFY(next, context);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        }
    }

    @Override
    protected void NOTIFY(GSOperationChain task, GSTPGContext context) {
        context.partitionStateManager.onOcExecuted(task);
    }

    /**
     * Used by GSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(GSTPGContext context, MyList<GSOperation> operation_chain, long mark_ID) {
        for (GSOperation operation : operation_chain) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    private GSOperationChain next(GSTPGContext context) {
        GSOperationChain operationChain = context.OCwithChildren.pollLast();
        if (operationChain == null) {
            return context.IsolatedOC.pollLast();
        }
        return operationChain;
    }

    /**
     * Distribute the operations to different threads with different strategies
     * 1. greedy: simply execute all operations has picked up.
     * 2. conserved: hash operations to threads based on the targeting key state
     * 3. shared: put all operations in a pool and
     */
    @Override
    public void DISTRIBUTE(GSOperationChain task, GSTPGContext context) {
        if (task != null)
            if (!task.hasChildren())
                context.IsolatedOC.add(task);
            else
                context.OCwithChildren.add(task);
    }

    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onExecutable(GSOperationChain operationChain) {
            DISTRIBUTE(operationChain, operationChain.context);//TODO: make it clear..
        }

        public void onOCFinalized(GSOperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }
    }
}
