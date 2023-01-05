package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.es.ESEvent;
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

public class CUBolt extends TransactionalBolt {
    SINKCombo sink;
    static AtomicInteger cuPostCount = new AtomicInteger(0);
    static AtomicInteger cuStopCount = new AtomicInteger(0);
    static ConcurrentSkipListSet<Integer> cuPostTweets = new ConcurrentSkipListSet<>();
    static ConcurrentSkipListSet<Double> cuPostEvents = new ConcurrentSkipListSet<>();

    public CUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_es";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void CLUSTER_UPDATE_REQUEST_POST(CUEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;
        String updatedClusterID = event.getClusterID();

        if (outBid >= total_events) { //Label stopping signals
            if (cuStopCount.incrementAndGet() == 16) {
                LOG.info("All stop signals are detected, posted tweets: " + cuPostTweets);
                LOG.info("All stop signals are detected, posted events: " + cuPostEvents);
            }
//            LOG.info("Thread " + thread_Id + " is posting stop signal " + outBid);
        }
        else {
            cuPostCount.incrementAndGet();
            cuPostTweets.add(Integer.parseInt(event.getTweetID()));
        }
        cuPostEvents.add(outBid);

        ESEvent outEvent = new ESEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), updatedClusterID);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, System.nanoTime());
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
