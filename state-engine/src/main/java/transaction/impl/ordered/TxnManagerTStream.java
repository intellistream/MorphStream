package transaction.impl.ordered;

import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import stage.Stage;
import storage.SchemaRecord;
import storage.StorageManager;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.impl.TxnManagerDedicatedAsy;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

import static content.common.CommonMetaTypes.AccessType.INSERT_ONLY;
import static transaction.context.TxnAccess.Access;

public class TxnManagerTStream extends TxnManagerDedicatedAsy {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerTStream.class);

    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw, String schedulerType, Stage stage) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw, numberOfStates, schedulerType, stage);
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            if (!tb_record.content_.TryWriteLock()) {
                return false;
            } else {
            }
            record.is_visible_ = true;
            Access access = access_list_.NewAccess();
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
    public void start_evaluate(int thread_Id, double mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(thread_Id);
        this.stage.getControl().preStateAccessBarrier(thread_Id);//sync for all threads of the same operator to come to this line to ensure the TPG is constructed for the current batch.
        this.stage.getScheduler().start_evaluation(context, mark_ID, num_events);
        this.stage.getControl().postStateAccessBarrier(thread_Id);
        MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(thread_Id);
        MeasureTools.SCHEDULE_TIME_RECORD(thread_Id, num_events);
    }
}
