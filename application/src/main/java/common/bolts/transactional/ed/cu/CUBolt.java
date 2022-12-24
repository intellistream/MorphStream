package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.es.ESEvent;
import common.param.ed.tr.TREvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class CUBolt extends TransactionalBolt {
    SINKCombo sink; //TODO:Default sink for measurement

    public CUBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_cu"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void CLUSTER_UPDATE_REQUEST_POST(CUEvent event) throws InterruptedException {

        double delta = 0.1;
        double outBid = event.getBid() + delta;
        String updatedClusterID = event.updatedClusterID;

        ESEvent outEvent = new ESEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), updatedClusterID);
        GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent);
        Tuple tuple = new Tuple(outEvent.getMyBid(), 0, context, generalMsg);

        LOG.info("Posting event: " + outBid);

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
