package intellistream.morphstream.engine.txn.transaction.impl.ordered;

import intellistream.morphstream.engine.db.storage.impl.StorageManager;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.impl.TxnManagerDedicatedAsy;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;

public class TxnManagerTStream extends TxnManagerDedicatedAsy {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerTStream.class);

    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw, String schedulerType) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw, numberOfStates, schedulerType);
    }

    /**
     * This is the API: SP-Layer inform the arrival of checkpoint, which informs the TP-Layer to start evaluation.
     *
     * @param operatorID
     * @param batchID
     * @param num_events
     * @param thread_Id
     * @param mark_ID
     * @return time spend in tp evaluation.
     */
    @Override
    public void start_evaluate(String operatorID, int batchID, int num_events, int thread_Id, long mark_ID) throws InterruptedException, BrokenBarrierException {
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        if (TxnManager.enableGroup) {
            TxnManager.schedulerByGroup.get(getGroupId(thread_Id)).start_evaluation(context, mark_ID, num_events, batchID);
        } else {
            TxnManager.scheduler.start_evaluation(context, mark_ID, num_events, batchID);
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
        //Sync to switch scheduler(more overhead) decide by the mark_ID or runtime information
        if (TxnManager.enableDynamic && TxnManager.collector.timeToSwitch(mark_ID, thread_Id, TxnManager.currentSchedulerType.get(thread_Id))) {
            String schedulerType = TxnManager.collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id, mark_ID, batchID, operatorID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
    }
}
