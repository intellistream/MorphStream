package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.param.ed.sc.SCEvent;
import common.param.ed.tc.TCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.*;
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

        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;
//        boolean isBurst = event.isBurst;

        if (!enable_app_combo) {
            // LOG.info("Posting event: " + outBid);
//                SCEvent outEvent = new SCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(),
//                        event.getMyNumberOfPartitions(), tweetID, isBurst);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

            collector.emit(outBid, tuple); //emit to TCG

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }

    protected void STOP_SIGNAL_POST(TCEvent event) throws InterruptedException {

//        double delta = 0.1;
        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;
//        boolean isBurst = event.isBurst;

//        LOG.info("Sending stop signal to SC: " + event.getBid());

        if (!enable_app_combo) {
//            SCEvent outEvent = new SCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(),
//                    event.getMyNumberOfPartitions(), "Stop", isBurst);
            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);
            collector.emit(outBid, tuple); //emit to SC

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }


}
