package intellistream.morphstream.engine.txn.transaction.impl.ordered;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.stage.Stage;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.impl.TxnManagerDedicatedAsy;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

public class TxnManagerTStream extends TxnManagerDedicatedAsy {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerTStream.class);

    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw, String schedulerType) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw, numberOfStates, schedulerType);
    }
//    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw, String schedulerType, Stage stage) {
//        super(storageManager, thisComponentId, thisTaskId, thread_countw, numberOfStates, schedulerType, stage);
//    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record, this.thread_count_);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            if (!tb_record.content_.TryWriteLock()) {
                return false;
            } else {
            }
            record.is_visible_ = true;
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = CommonMetaTypes.AccessType.INSERT_ONLY;
            access.access_record_ = tb_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = 0;
            return true;
        } else {
            return true;
        }
    }

    /**
     * This is the API: SP-Layer inform the arrival of checkpoint, which informs the TP-Layer to start evaluation.
     *
     * @param thread_Id
     * @param mark_ID
     * @param num_events
     * @return time spend in tp evaluation.
     */
    @Override
    public void start_evaluate(int thread_Id, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
//        this.stage.getControl().preStateAccessBarrier(thread_Id);
        if (TxnManager.enableGroup) {
            TxnManager.schedulerByGroup.get(getGroupId(thread_Id)).start_evaluation(context, mark_ID, num_events);
            //TODO: Replace with code below for stage
//            this.stage.getScheduler().start_evaluation(context, mark_ID, num_events);
        } else {
            TxnManager.scheduler.start_evaluation(context, mark_ID, num_events);
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
//        this.stage.getControl().postStateAccessBarrier(thread_Id);
        //Sync to switch scheduler(more overhead) decide by the mark_ID or runtime information
        if (TxnManager.enableDynamic && TxnManager.collector.timeToSwitch(mark_ID, thread_Id, TxnManager.currentSchedulerType.get(thread_Id))) {
            String schedulerType = TxnManager.collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id, mark_ID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
    }
}
