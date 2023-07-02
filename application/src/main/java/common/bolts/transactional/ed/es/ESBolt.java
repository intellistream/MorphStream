package common.bolts.transactional.ed.es;

import combo.SINKCombo;
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

public class ESBolt extends TransactionalBolt {
    SINKCombo sink;

    static AtomicInteger esPostCount = new AtomicInteger(0);
    static AtomicInteger esStopCount = new AtomicInteger(0);
    static ConcurrentSkipListSet<Integer> esPostClusters = new ConcurrentSkipListSet<>();
    static ConcurrentSkipListSet<Double> esPostEvents = new ConcurrentSkipListSet<>();

    public ESBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_es";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void EVENT_SELECT_REQUEST_POST(ESEvent event) throws InterruptedException {
        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;
//        LOG.info("Posting event: " + outBid);

        if (outBid >= total_events) { //Label stopping signals
            if (esStopCount.incrementAndGet() == tthread*tthread) {
                LOG.info("All stop signals are detected, posted clusters: " + esPostClusters);
                LOG.info("All stop signals are detected, posted events: " + esPostEvents);
            }
//            LOG.info("Thread " + thread_Id + " is posting stop signal " + outBid);
        }
        else {
            esPostCount.incrementAndGet();
            esPostClusters.add(Integer.parseInt(event.getClusterID()));
        }
        esPostEvents.add(outBid);

        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, event.getTimestamp());
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
