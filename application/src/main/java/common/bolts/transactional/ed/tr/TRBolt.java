package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import common.param.ed.wu.WUEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class TRBolt extends TransactionalBolt {

    SINKCombo sink; //Default sink for measurement

    private int postCount = 0;

    public TRBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tr"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TWEET_REGISTRANT_REQUEST_POST(TREvent event) throws InterruptedException {

        String tweetID = event.getTweetID();
        String[] words = event.getWords();
        double delta = 0.1;

        int outBid = event.getMyBid(); //TODO: Add delta

        for (String word : words) {

            WUEvent outEvent = new WUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), word, tweetID);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, System.nanoTime());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

//            LOG.info("Posting event: " + event.getBid() + ", Counter = " + postCount);
            postCount++;
            if (outBid >= 30) {
                LOG.info("Posting event: " + event.getBid());
            }

            if (!enable_app_combo) {
//                collector.emit(outBid, tuple, event.getTimestamp());
                collector.emit(outBid, tuple);
            } else {
                if (enable_latency_measurement) {
                    //Pass the read result of new tweet's ID (assigned by table) to sink
                    sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
                }
            }
        }


    }

}
