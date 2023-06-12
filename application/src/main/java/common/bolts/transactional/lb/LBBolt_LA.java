package common.bolts.transactional.lb;

import combo.SINKCombo;
import common.param.lb.LBEvent;
import db.DatabaseException;
import org.slf4j.Logger;

import static profiler.MeasureTools.*;

public abstract class LBBolt_LA extends LBBolt {
    public LBBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    protected void LAL(LBEvent event, double i, double _bid) throws DatabaseException {
        LB_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
    }

    protected void PostLAL_process(double _bid) throws DatabaseException, InterruptedException {
        //txn process phase.
        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
            LBEvent event = (LBEvent) input_event;
            lb_request_noLock(event, txn_context[(int) (i - _bid)]);
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            LB_LA_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }

//    @Override
//    protected void LAL_PROCESS(double _bid) throws InterruptedException, DatabaseException {
//        BEGIN_WAIT_TIME_MEASURE(thread_Id);
//        //ensures that locks are added in the input_event sequence order.
//        transactionManager.getOrderLock().blocking_wait(_bid);
//        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        IBWJ_LOCK_AHEAD((IBWJEvent) input_event, txn_context[0]);
//        END_LOCK_TIME_MEASURE(thread_Id);
//        transactionManager.getOrderLock().advance();
//        END_WAIT_TIME_MEASURE(thread_Id);
//    }
//
//    protected void PostLAL_process(double _bid) throws DatabaseException, InterruptedException {
//        int _combo_bid_size = 1;
//        //txn process phase.
//        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
//            IBWJ_REQUEST_NOLOCK((IBWJEvent) input_event, txn_context[(int) (i - _bid)]); //read requested record's reference from table
//            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//            IBWJ_REQUEST_CORE((IBWJEvent) input_event); //txn processing
//            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
//        }
//    }
}
