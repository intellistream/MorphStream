package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
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

public abstract class TCGBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"

    public TCGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tcg";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TC_GATE_REQUEST_POST(double bid, CUEvent event) throws InterruptedException {

        double outBid = Math.round(bid * 10.0) / 10.0;
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        LOG.info("Posting event: " + outBid);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
