package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.sc.SCEvent;
import common.param.ed.tc.TCEvent;
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


public abstract class TCGBolt extends TransactionalBolt {

    SINKCombo sink;
//    static ConcurrentSkipListSet<Integer> tweetIDSet = new ConcurrentSkipListSet<>();
//    public static ConcurrentSkipListSet<Double> tcgPostEvents = new ConcurrentSkipListSet<>();
//    public static AtomicInteger tcgStopEvents = new AtomicInteger(0);

    public TCGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tcg";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TC_GATE_REQUEST_POST(TCEvent event, boolean isBurst) throws InterruptedException {

        double delta = 0.1;
        double outBid = Math.round((event.getMyBid() + delta) * 10.0) / 10.0;

//        if (!(outBid >= total_events)) {
//            tweetIDSet.add(Integer.parseInt(event.getTweetID()));
//            tcgPostEvents.add(outBid);
//        }
//        else {
//            LOG.info("Thread " + thread_Id + " posting stop event: " + outBid);
//            if (tcgStopEvents.incrementAndGet() == tthread*tthread) {
//                LOG.info("TCG post tweets: " + tweetIDSet);
//                LOG.info("TCG post events: " + tcgPostEvents);
//                LOG.info("TCG stop events: " + tcgStopEvents);
//            }
//        }
//        LOG.info("Thread " + thread_Id + " posting event: " + outBid);

        SCEvent outEvent = new SCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(),
                event.getMyNumberOfPartitions(), event.getTweetID(), isBurst);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent, event.getTimestamp());
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}