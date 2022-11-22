package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.tr.TREvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class CUBolt extends TransactionalBolt {
    SINKCombo sink; //TODO:Default sink for measurement

    public CUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_cu"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            ((CUEvent) input_event).setTimestamp(timestamp);
            CLUSTER_UPDATE_REQUEST_POST((CUEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void CLUSTER_UPDATE_REQUEST_POST(CUEvent event) throws InterruptedException {

        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
        Tuple tuple = new Tuple(event.getMyBid(), 0, context, generalMsg);

        if (!enable_app_combo) {
            collector.emit(event.getMyBid(), tuple, event.getTimestamp());//emit CU Event tuple to CU Gate
        } else {
            if (enable_latency_measurement) {
                //Pass the information to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
