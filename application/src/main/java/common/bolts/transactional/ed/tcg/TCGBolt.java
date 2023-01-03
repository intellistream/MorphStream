package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.sc.SCEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.TreeSet;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class TCGBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"
    int counter = 0;
    TreeSet<Integer> tweetIDSet = new TreeSet<>();

    public TCGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_tcg";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TC_GATE_REQUEST_POST(double bid, SCEvent event) throws InterruptedException {

        double outBid = Math.round(bid * 10.0) / 10.0;
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
        Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

        counter++;
//        LOG.info("Posting event: " + bid + ", Total post: " + counter);

        if (!Objects.equals(event.getTweetID(), "Stop")) {
            tweetIDSet.add(Integer.parseInt(event.getTweetID()));
        }
        if (counter >= 199) {
//            LOG.info("Posted all tweetIDs");
        }

        if (!enable_app_combo) {
            collector.emit(outBid, tuple);
        } else {
            if (enable_latency_measurement) {
                //Pass the read result of new tweet's ID (assigned by table) to sink
                sink.execute(new Tuple(bid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
