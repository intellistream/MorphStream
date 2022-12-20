package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public class TCBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"

    public TCBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tc"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TREND_CALCULATE_REQUEST_POST(TCEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = event.getMyBid() + delta;

        if (!enable_app_combo) {

            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());

            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

            LOG.info("Posting event: " + outBid);

            collector.emit(outBid, tuple);//emit CU Event tuple to TC Gate

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }


}
