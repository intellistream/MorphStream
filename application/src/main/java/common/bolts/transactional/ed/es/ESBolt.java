package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import common.param.ed.es.ESEvent;
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

public class ESBolt extends TransactionalBolt {
    SINKCombo sink;

    public ESBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_es";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void EVENT_SELECT_REQUEST_POST(ESEvent event) throws InterruptedException {
        boolean isEvent = event.isEvent;
        String[] wordList = event.wordList;
        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;
        LOG.info("Posting event: " + outBid);

        if (!enable_app_combo) {
            String output = "";
            if (isEvent) {
                //create output event information
                output = event.getClusterID() + String.join(",", wordList);
            } else {
                //create output non-event information
                output = event.getClusterID() + "is not an Event.";
            }

            //the second argument should be event detection output
            collector.emit(outBid, output);//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
