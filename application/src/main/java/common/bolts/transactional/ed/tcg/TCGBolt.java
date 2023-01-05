package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.sc.SCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class TCGBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"
    static ConcurrentSkipListSet<Integer> tweetIDSet = new ConcurrentSkipListSet<>();
    public static ConcurrentSkipListSet<Double> tcgPostEvents = new ConcurrentSkipListSet<>();
    public static AtomicInteger tcgStopEvents = new AtomicInteger(0);

    public TCGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tcg";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TC_GATE_REQUEST_POST(double bid, SCEvent event) throws InterruptedException {

        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
        Tuple tuple = new Tuple(bid, 0, context, generalMsg);

        if (!Objects.equals(event.getTweetID(), "Stop")) {
            tweetIDSet.add(Integer.parseInt(event.getTweetID()));
            tcgPostEvents.add(bid);
        }
        else {
            LOG.info("Thread " + thread_Id + " posting stop event: " + bid);
            if (tcgStopEvents.incrementAndGet() == 16) {
                LOG.info("TCG post tweets: " + tweetIDSet);
                LOG.info("TCG post events: " + tcgPostEvents);
                LOG.info("TCG stop events: " + tcgStopEvents);
            }
        }

        LOG.info("Thread " + thread_Id + " posting event: " + bid);

        if (!enable_app_combo) {
            collector.emit(bid, tuple);
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(bid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
