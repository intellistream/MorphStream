package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.tc.TCEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import utils.AppConfig;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class TCBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"

    public TCBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tc"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            ((TCEvent) input_event).setTimestamp(timestamp);
            TREND_CALCULATE_REQUEST_POST((TCEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TREND_CALCULATE_REQUEST_POST(TCEvent event) throws InterruptedException {

        //Read isBurst result from TCEvent
        boolean isBurst = event.isBurst;

        if (!enable_app_combo) {
            //TODO: Increase bid by delta, emit new_bid, tweetID, and isBurst
            for (String tweetID : event.tweetIDList) {
                //TODO: Initialize a new CU Event and pass to collector, pass isBurst to CU Event
                collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
            }
        } else {
            if (enable_latency_measurement) {
                //TODO: Pass computation result "true" to sink to measure performance
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }


}
