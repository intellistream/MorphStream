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
            LB_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
        }
    }

}
