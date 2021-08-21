package common.bolts.transactional.ob;

import common.param.TxnEvent;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.ToppingEvent;
import combo.SINKCombo;
import db.DatabaseException;
import org.slf4j.Logger;
import transaction.context.TxnContext;

import static profiler.MeasureTools.*;

public abstract class OBBolt_LA extends OBBolt {
    int _combo_bid_size = 1;

    public OBBolt_LA(Logger log, int fid, SINKCombo sink) {
        super(log, fid, sink);
    }

    protected void LAL(Object event, long i, long _bid) throws DatabaseException {
        if (event instanceof BuyingEvent) {
            BUYING_REQUEST_LOCKAHEAD((BuyingEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof AlertEvent) {
            ALERT_REQUEST_LOCKAHEAD((AlertEvent) event, txn_context[(int) (i - _bid)]);
        } else if (event instanceof ToppingEvent) {
            TOPPING_REQUEST_LOCKAHEAD((ToppingEvent) event, txn_context[(int) (i - _bid)]);
        } else {
            LOG.error("Wrong");
            System.exit(-1);
        }
    }

    //lock_ratio-ahead phase.
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
            if (event instanceof BuyingEvent) {
                BUYING_REQUEST_NOLOCK((BuyingEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                BUYING_REQUEST_CORE((BuyingEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else if (event instanceof AlertEvent) {
                ALERT_REQUEST_NOLOCK((AlertEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                ALERT_REQUEST_CORE((AlertEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            } else {
                TOPPING_REQUEST_NOLOCK((ToppingEvent) event, txn_context[(int) (i - _bid)]);
                BEGIN_ACCESS_TIME_MEASURE(thread_Id);
                TOPPING_REQUEST_CORE((ToppingEvent) event);
                END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            }
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
