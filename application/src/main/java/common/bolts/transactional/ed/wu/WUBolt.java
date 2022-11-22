package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import common.param.ed.wu.WUEvent;
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

public class WUBolt extends TransactionalBolt {
    SINKCombo sink; //Default sink for measurement

    public WUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_wu"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void WORD_UPDATE_REQUEST_POST(WUEvent event) throws InterruptedException {

        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
        Tuple tuple = new Tuple(event.getMyBid(), 0, context, generalMsg);

        if (!enable_app_combo) {
            collector.emit(event.getMyBid(), tuple, event.getTimestamp());//emit WU Event tuple to WU Gate
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
