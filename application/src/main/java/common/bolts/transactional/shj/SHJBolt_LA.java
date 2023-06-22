package common.bolts.transactional.shj;

import combo.SINKCombo;
import common.param.shj.SHJEvent;
import db.DatabaseException;
import org.slf4j.Logger;
import transaction.context.TxnContext;

import static profiler.MeasureTools.*;

public abstract class SHJBolt_LA extends SHJBolt {
    public SHJBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws InterruptedException, DatabaseException {
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        SHJ_LOCK_AHEAD((SHJEvent) input_event, txn_context[0]);
        END_LOCK_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }

    protected void PostLAL_process(double _bid) throws DatabaseException, InterruptedException {
        int _combo_bid_size = 1;
        //txn process phase.
        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
            SHJ_REQUEST_NOLOCK((SHJEvent) input_event, txn_context[(int) (i - _bid)]); //read requested record's reference from table
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            SHJ_REQUEST_CORE((SHJEvent) input_event); //txn processing
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
