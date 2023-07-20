package intellistream.morphstream.examples.tsp.onlinebiding.op;

import intellistream.morphstream.examples.tsp.onlinebiding.events.AlertTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.ToppingTxnEvent;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.examples.utils.SINKCombo;
import org.slf4j.Logger;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;

public abstract class OBBolt_LA extends OBBolt {
    int _combo_bid_size = 1;

    public OBBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    protected void LAL(Object event, long i, long _bid) throws DatabaseException {
        if (event instanceof BuyingTxnEvent) {
            BUYING_REQUEST_LOCKAHEAD((BuyingTxnEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof AlertTxnEvent) {
            ALERT_REQUEST_LOCKAHEAD((AlertTxnEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof ToppingTxnEvent) {
            TOPPING_REQUEST_LOCKAHEAD((ToppingTxnEvent) event, txn_context[(int) (i - _bid)]);
        } else {
            if (enable_log) LOG.error("Wrong");
            System.exit(-1);
        }
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        transactionManager.getOrderLock().blocking_wait(_bid);
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        LAL(input_event, 0, _bid);
        END_LOCK_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }

    protected void PostLAL_process(long _bid) throws DatabaseException {
        //txn process phase.
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            TxnEvent event = (TxnEvent) input_event;
            if (event instanceof BuyingTxnEvent) {
                BUYING_REQUEST_NOLOCK((BuyingTxnEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                BUYING_REQUEST_CORE((BuyingTxnEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else if (event instanceof AlertTxnEvent) {
                ALERT_REQUEST_NOLOCK((AlertTxnEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                ALERT_REQUEST_CORE((AlertTxnEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else {
                TOPPING_REQUEST_NOLOCK((ToppingTxnEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                TOPPING_REQUEST_CORE((ToppingTxnEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
