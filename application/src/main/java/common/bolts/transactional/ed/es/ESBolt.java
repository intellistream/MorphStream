package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import common.param.ed.es.ESEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

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

        if (!enable_app_combo) {
            String output = "";
            if (isEvent) {
                //create output event information
                output = event.getClusterID() + String.join(",", wordList);
            } else {
                //create output non-event information
                output = event.getClusterID() + "is not an Event.";
            }

            LOG.info("Posting event: " + event.getMyBid());

            //the second argument should be event detection output
            collector.emit(event.getBid(), output);//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
