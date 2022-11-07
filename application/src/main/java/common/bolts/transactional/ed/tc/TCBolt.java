package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
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

    //TODO: Referred from SLBolt, need double-check
    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            ((TCEvent) input_event).setTimestamp(timestamp);
            TC_REQUEST_POST((TCEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TC_REQUEST_POST(TCEvent event) throws InterruptedException {
        double tfIdf = 0;
        double tf = (double) event.frequency / event.windowSize;
        double idf = -1 * (Math.log((double) event.countOccurWindow / event.windowCount));
        tfIdf = tf * idf;
        if (!enable_app_combo) {
            //TODO: Increase bid by delta, compare with old tf-idf, and update tfIdf value of word
            collector.emit(event.getBid(), tfIdf, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                //TODO: Pass computation result "true" to sink to measure performance
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, tfIdf, event.getTimestamp())));
            }
        }
    }


}
