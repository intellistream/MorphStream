package common.bolts.transactional.lb;

import combo.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combine Read-Write for nocc.
 */
public class LBBolt_Locks extends LBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LBBolt_Locks.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public LBBolt_Locks(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

//    protected void write_txn_process(IBWJEvent event, double i, double _bid) throws DatabaseException, InterruptedException {
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        boolean success = write_request(event, txn_context[(int) (i - _bid)]);
//        if (success) {
//            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//            WRITE_CORE(event);
//            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//        } else {//being aborted.
//            txn_context[(int) (i - _bid)].is_retry_ = true;
//            BEGIN_ABORT_TIME_MEASURE(thread_Id);
//            while (!write_request(event, txn_context[(int) (i - _bid)]) && !Thread.currentThread().isInterrupted()) ;
//            END_ABORT_TIME_MEASURE_ACC(thread_Id);
//            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//            WRITE_CORE(event);
//            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//        }
//    }
//
//    protected void read_txn_process(IBWJEvent event, double i, double _bid) throws DatabaseException, InterruptedException {
//        boolean success = read_request(event, txn_context[(int) (i - _bid)]);
//        if (success) {
//            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//            READ_CORE(event);
//            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//        } else {//being aborted.
//            txn_context[(int) (i - _bid)].is_retry_ = true;
//            BEGIN_ABORT_TIME_MEASURE(thread_Id);
//            while (!read_request(event, txn_context[(int) (i - _bid)])) ;
//            END_ABORT_TIME_MEASURE_ACC(thread_Id);
//            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//            READ_CORE(event);
//            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
//        }
//    }
//
//    @Override
//    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
//        for (double i = _bid; i < _bid + combo_bid_size; i++) {
//            IBWJEvent event = (IBWJEvent) input_event;
//            boolean flag = event.READ_EVENT();
//            if (flag) {
//                read_txn_process(event, i, _bid);
//            } else {
//                write_txn_process(event, i, _bid);
//            }
//        }
//    }
//
//    @Override
//    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
//        super.initialize(thread_Id, thisTaskId, graph);
//        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), transactionManager.stage);
//    }
//
//    @Override
//    public void execute(Tuple in) throws InterruptedException, DatabaseException {
//        nocc_execute(in);
//    }
}