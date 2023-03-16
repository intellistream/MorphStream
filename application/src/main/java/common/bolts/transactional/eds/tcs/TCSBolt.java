package common.bolts.transactional.eds.tcs;

import combo.SINKCombo;
import common.param.eds.tcs.TCSEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public class TCSBolt extends TransactionalBolt {
    SINKCombo sink;

    public TCSBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "eds_tcs";
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void TREND_CALCULATE_REQUEST_POST(TCSEvent event) throws InterruptedException {

        double outBid = Math.round(event.getMyBid() * 10.0) / 10.0;

        if (!enable_app_combo) {
//            LOG.info("Thread " + thread_Id + " posting event: " + outBid);

            GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, event.getTimestamp());
            Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

            collector.emit(outBid, tuple); //emit to TCG
//            LOG.info("Threads " + thread_Id + " posted event count: " + threadPostCount.incrementAndGet());

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(outBid, this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }
}
