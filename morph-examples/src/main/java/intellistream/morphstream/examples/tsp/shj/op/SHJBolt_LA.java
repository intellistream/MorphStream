package intellistream.morphstream.examples.tsp.shj.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.shj.events.SHJTxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;

import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;

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
        SHJ_LOCK_AHEAD((SHJTxnEvent) input_event, txn_context[0]);
        END_LOCK_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().advance();
        END_WAIT_TIME_MEASURE(thread_Id);
    }

    protected void PostLAL_process(double _bid) throws DatabaseException, InterruptedException {
        int _combo_bid_size = 1;
        //txn process phase.
        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
            SHJ_REQUEST_NOLOCK((SHJTxnEvent) input_event, txn_context[(int) (i - _bid)]); //read requested record's reference from table
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            SHJ_REQUEST_CORE((SHJTxnEvent) input_event); //txn processing
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }
}
