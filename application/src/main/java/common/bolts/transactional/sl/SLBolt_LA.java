package common.bolts.transactional.sl;

import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import common.sink.SINKCombo;
import db.DatabaseException;
import org.slf4j.Logger;
import transaction.context.TxnContext;

import static profiler.MeasureTools.*;

public class SLBolt_LA extends SLBolt {
    public SLBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {
        int _combo_bid_size = 1;
        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            if (input_event instanceof DepositEvent) {
                DEPOSITE_REQUEST_NOLOCK((DepositEvent) input_event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                DEPOSITE_REQUEST_CORE((DepositEvent) input_event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else {
                TRANSFER_REQUEST_NOLOCK((TransactionEvent) input_event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                TRANSFER_REQUEST_CORE((TransactionEvent) input_event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws InterruptedException, DatabaseException {
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        if (input_event instanceof DepositEvent) {
            DEPOSITE_LOCK_AHEAD((DepositEvent) input_event, txn_context[0]);
        } else if (input_event instanceof TransactionEvent) {
            TRANSFER_LOCK_AHEAD((TransactionEvent) input_event, txn_context[0]);
        } else {
//            throw new UnsupportedOperationException();
            LOG.error("Unsupported");
            System.exit(-1);
        }
        END_LOCK_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }
}
