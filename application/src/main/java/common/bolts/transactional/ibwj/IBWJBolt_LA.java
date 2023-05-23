package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import common.param.ibwj.IBWJEvent;
import db.DatabaseException;
import org.slf4j.Logger;
import transaction.context.TxnContext;

import static profiler.MeasureTools.*;

public abstract class IBWJBolt_LA extends IBWJBolt {
    public IBWJBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

//    protected void LAL(IBWJEvent event, double i, double _bid) throws DatabaseException {
//        boolean flag = event.READ_EVENT();
//        if (flag) {//read
//            READ_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
//        } else {
//            WRITE_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
//        }
//    }
//
//    //lock_ratio-ahead phase.
//    @Override
//    protected void LAL_PROCESS(double _bid) throws DatabaseException, InterruptedException {
//        BEGIN_WAIT_TIME_MEASURE(thread_Id);
//        //ensures that locks are added in the input_event sequence order.
//        transactionManager.getOrderLock().blocking_wait(_bid);
//        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
//        IBWJEvent event = (IBWJEvent) input_event;
//        LAL(event, 0, _bid);
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        END_LOCK_TIME_MEASURE(thread_Id);
//        transactionManager.getOrderLock().advance();
//        END_WAIT_TIME_MEASURE(thread_Id);
//    }
//
//    protected void PostLAL_process(double _bid) throws DatabaseException, InterruptedException {
//        //txn process phase.
//        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
//            IBWJEvent event = (IBWJEvent) input_event;
//            boolean flag = event.READ_EVENT();
//            if (flag) {//read
//                read_request_noLock(event, txn_context[(int) (i - _bid)]);
//                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//                READ_CORE(event);
//                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            } else {
//                write_request_noLock(event, txn_context[(int) (i - _bid)]);
//                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
//                WRITE_CORE(event);
//                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
//            }
//            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
//        }
//    }
}
