package common.bolts.transactional.ed.trg;

import combo.SINKCombo;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class TRGBolt extends TransactionalBolt {

    SINKCombo sink; //Default sink for measurement

    public TRGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_trg"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TR_GATE_REQUEST_POST(WUEvent event) throws InterruptedException {

        double outBid = event.getMyBid();
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        LOG.info("Posting event: " + outBid);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
