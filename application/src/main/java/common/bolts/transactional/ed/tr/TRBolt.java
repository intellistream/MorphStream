package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class TRBolt extends TransactionalBolt {

    SINKCombo sink; //TODO:Default sink for measurement

    public TRBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tr"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            ((TREvent) input_event).setTimestamp(timestamp);
            TWEET_REGISTRANT_REQUEST_POST((TREvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TWEET_REGISTRANT_REQUEST_POST(TREvent event) throws InterruptedException {
        //TODO: Refer to GSWBolt, we can perform some correctness measurement here
//        int sum = 0; //Pass this sum value to sink for measurement
//        if (POST_COMPUTE_COMPLEXITY != 0) {
//            for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
//                sum += event.result.get(i);
//            }
//            for (int j = 0; j < POST_COMPUTE_COMPLEXITY; ++j)
//                sum += System.nanoTime();
//        }
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.tweetIDResult, event.getTimestamp())));
            }
        }
    }

}
