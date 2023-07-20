package intellistream.morphstream.examples.tsp.streamledger.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.streamledger.events.DepositTxnEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.TransactionTxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;

public class SLBolt_LA extends SLBolt {
    public SLBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    protected void PostLAL_process(long _bid) throws DatabaseException, InterruptedException {
        int _combo_bid_size = 1;
        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            if (input_event instanceof DepositTxnEvent) {
                DEPOSITE_REQUEST_NOLOCK((DepositTxnEvent) input_event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                DEPOSITE_REQUEST_CORE((DepositTxnEvent) input_event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else {
                TRANSFER_REQUEST_NOLOCK((TransactionTxnEvent) input_event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                TRANSFER_REQUEST_CORE((TransactionTxnEvent) input_event);
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
        if (input_event instanceof DepositTxnEvent) {
            DEPOSITE_LOCK_AHEAD((DepositTxnEvent) input_event, txn_context[0]);
        } else if (input_event instanceof TransactionTxnEvent) {
            TRANSFER_LOCK_AHEAD((TransactionTxnEvent) input_event, txn_context[0]);
        } else {
//            throw new UnsupportedOperationException();
            if (enable_log) LOG.error("Unsupported");
            System.exit(-1);
        }
        END_LOCK_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }
}
