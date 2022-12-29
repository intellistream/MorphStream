package common.bolts.transactional.ed.sc;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.sc.SCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public class SCBolt extends TransactionalBolt {
    SINKCombo sink;

    public SCBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_cu";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void SIMILARITY_CALCULATE_REQUEST_POST(SCEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String tweetID = event.getTweetID();
        String targetClusterID = event.targetClusterID;

        CUEvent outEvent = new CUEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), tweetID, targetClusterID);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent);
        Tuple tuple = new Tuple(outEvent.getMyBid(), 0, context, generalMsg);

//        LOG.info("Posting event: " + outBid);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);//emit CU Event tuple to CU Gate
        } else {
            if (enable_latency_measurement) {
                //Pass the information to sink
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
