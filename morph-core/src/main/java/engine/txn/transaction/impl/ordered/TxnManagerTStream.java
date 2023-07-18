package engine.txn.transaction.impl.ordered;

import engine.txn.db.DatabaseException;
import engine.txn.profiler.MeasureTools;
import engine.txn.transaction.TxnManager;
import engine.txn.transaction.context.TxnAccess;
import engine.txn.transaction.context.TxnContext;
import engine.txn.transaction.impl.TxnManagerDedicatedAsy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.storage.SchemaRecord;
import engine.txn.storage.StorageManager;
import engine.txn.storage.TableRecord;
import engine.txn.utils.SOURCE_CONTROL;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

import static engine.txn.content.common.CommonMetaTypes.AccessType.INSERT_ONLY;

public class TxnManagerTStream extends TxnManagerDedicatedAsy {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerTStream.class);
    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw, String schedulerType) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw, numberOfStates, schedulerType);
    }

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
            access.access_type_ = INSERT_ONLY;
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
        MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(thread_Id);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        if (TxnManager.enableGroup) {
            TxnManager.schedulerByGroup.get(getGroupId(thread_Id)).start_evaluation(context, mark_ID, num_events);
        } else {
            TxnManager.scheduler.start_evaluation(context, mark_ID, num_events);
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
        MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(thread_Id);
        MeasureTools.SCHEDULE_TIME_RECORD(thread_Id, num_events);
        //Sync to switch scheduler(more overhead) decide by the mark_ID or runtime information
        MeasureTools.BEGIN_SCHEDULER_SWITCH_TIME_MEASURE(thread_Id);
        if (TxnManager.enableDynamic && TxnManager.collector.timeToSwitch(mark_ID,thread_Id, TxnManager.currentSchedulerType.get(thread_Id))){
            String schedulerType = TxnManager.collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id,mark_ID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
        MeasureTools.END_SCHEDULER_SWITCH_TIME_MEASURE(thread_Id);
    }
}
