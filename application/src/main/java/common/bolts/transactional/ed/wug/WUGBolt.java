package common.bolts.transactional.ed.wug;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class WUGBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"

    public WUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_wug"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void WU_GATE_REQUEST_POST(WUEvent event) throws InterruptedException, DatabaseException {

        double delta = 0.1;
        double outBid = event.getMyBid() + delta;

        if (!enable_app_combo) {
            String wordID = event.getWordID();

            TCEvent outEvent = new TCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), wordID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, System.nanoTime());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

            LOG.info("Posting event: " + outBid);

            collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getMyBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
