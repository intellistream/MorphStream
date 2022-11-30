package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Marker;
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
        this.configPrefix = "ed_tcg"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void TC_GATE_REQUEST_POST(CUEvent event) throws InterruptedException {

        int outBid = event.getMyBid();
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        LOG.info("Posting event: " + event.getMyBid());

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

    protected void insertMaker(CUEvent event) throws InterruptedException {

        int outBid = event.getMyBid();
        //sourceID is used in ???, myIteration is used in SStore
        Tuple marker = new Tuple(outBid, 0, context, new Marker(DEFAULT_STREAM_ID, -1, outBid, 0));

        LOG.info("Inserting marker: " + event.getMyBid());

        if (!enable_app_combo) {
            collector.emit(outBid, marker);//tuple should be the input of next bolt's execute() method
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, "Maker", event.getTimestamp())));
            }
        }
    }

}
