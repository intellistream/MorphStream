package intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured;

import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSContext;
import intellistream.morphstream.engine.txn.scheduler.struct.og.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.FaultToleranceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_path;

public class OGNSScheduler extends AbstractOGNSScheduler<OGNSContext> {
    private static final Logger log = LoggerFactory.getLogger(OGNSScheduler.class);

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public AtomicBoolean needAbortHandling = new AtomicBoolean(false);

    public OGNSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(OGNSContext context) {
        needAbortHandling.compareAndSet(true, false);
        tpg.firstTimeExploreTPG(context);
        if (tpg.isLogging == LOGOption_path && FaultToleranceRelax.isSelectiveLogging) {
            this.loggingManager.selectiveLoggingPartition(context.thisThreadId);
        }
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }

    public void REINITIALIZE(OGNSContext context) {
        tpg.secondTimeExploreTPG(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        needAbortHandling.compareAndSet(true, false);
    }

    @Override
    public void start_evaluation(OGNSContext context, long mark_ID, int num_events, int batchID) {
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID, batchID);
        } while (!FINISHED(context));
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (needAbortHandling.get()) {
            if (enable_log) {
                log.info("need abort handling, rollback and redo");
            }
            REINITIALIZE(context);
            do {
                EXPLORE(context);
                PROCESS(context, mark_ID, batchID);
            } while (!FINISHED(context));
        }
        RESET(context);
    }

    /**
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(OGNSContext context) {
        context.partitionStateManager.handleStateTransitions();
    }

    @Override
    protected void NOTIFY(OperationChain task, OGNSContext context) {
        context.partitionStateManager.onOcExecuted(task);
    }

    @Override
    protected void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        // in coarse-grained algorithms, we will not handle transaction abort gracefully, just update the state of the operation
        operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
        if (isLogging == FaultToleranceConstants.LOGOption_path && operation.getTxnOpId() == 0) {
            MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
            this.tpg.threadToPathRecord.get(operationChain.context.thisThreadId).addAbortBid(operation.bid);
            MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
        }
        // save the abort information and redo the batch.
        needAbortHandling.compareAndSet(false, true);
    }


    /**
     * Register an operation to queue.
     */
    public class ExecutableTaskListener {
        public void onOCExecutable(OperationChain operationChain) {
            DISTRIBUTE(operationChain, (OGNSContext) operationChain.context);//TODO: make it clear..
        }

        public void onOCFinalized(OperationChain operationChain) {
            operationChain.context.scheduledOPs += operationChain.getOperations().size();
        }

        public void onOCRollbacked(OperationChain operationChain) {
            operationChain.context.scheduledOPs -= operationChain.getOperations().size();
        }
    }
}
