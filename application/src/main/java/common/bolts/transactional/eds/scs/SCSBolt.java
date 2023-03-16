package common.bolts.transactional.eds.scs;

import combo.SINKCombo;
import common.param.eds.cus.CUSEvent;
import common.param.eds.scs.SCSEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;


public class SCSBolt extends TransactionalBolt {
    SINKCombo sink;
    static AtomicInteger scPostCount = new AtomicInteger(0);
    static AtomicInteger scStopCount = new AtomicInteger(0);
    static ConcurrentSkipListSet<Integer> scPostTweets = new ConcurrentSkipListSet<>();
    static ConcurrentSkipListSet<Double> scPostEvents = new ConcurrentSkipListSet<>();


    public SCSBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "eds_scs";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //TODO: Create a shared array that stores a copy of all cluster records to be iterated.
    // This cluster record array should be updated upon the arrival of the first event in each new window.
    // In this way, the cluster records only need to be read once from table.

    protected void SIMILARITY_CALCULATE_REQUEST_POST(SCSEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String tweetID = event.getTweetID();
        String targetClusterID = event.targetClusterID;

        if (outBid >= total_events) { //Label stopping signals
            targetClusterID = "Stop";
            if (scStopCount.incrementAndGet() == 16) {
                LOG.info("All stop signals are detected, posted tweets: " + scPostTweets);
                LOG.info("All stop signals are detected, posted events: " + scPostEvents);
            }
//            LOG.info("Thread " + thread_Id + " is posting stop signal " + outBid);
        }
        else {
            scPostCount.incrementAndGet();
            scPostTweets.add(Integer.parseInt(tweetID));
        }
        scPostEvents.add(outBid);

        if (targetClusterID == null) { //SC fail to find the most similar cluster
            throw new NullPointerException();
        }

        CUSEvent outEvent = new CUSEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), tweetID, targetClusterID);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
        Tuple tuple = new Tuple(outEvent.getMyBid(), 0, context, generalMsg);

//        LOG.info("Thread " + thread_Id + " is posting event: " + outBid);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }
}
