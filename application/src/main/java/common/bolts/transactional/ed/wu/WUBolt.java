package common.bolts.transactional.ed.wu;

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

public class WUBolt extends TransactionalBolt {
    SINKCombo sink;

    public WUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_wu";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void WORD_UPDATE_REQUEST_POST(WUEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String tweetID = event.getTweetID();

        if (!enable_app_combo) {
            String wordID = event.getWordID();

            TCEvent outEvent = new TCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), wordID, tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Posting event: " + outBid);

            collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }
}
