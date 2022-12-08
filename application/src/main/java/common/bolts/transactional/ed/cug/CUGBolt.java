package common.bolts.transactional.ed.cug;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.es.ESEvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import storage.StorageManager;

import java.util.Iterator;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;

public abstract class CUGBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"

    public CUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_cug"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    protected void CU_GATE_REQUEST_POST(CUEvent event) throws InterruptedException, DatabaseException {

        StorageManager storageManager = db.getStorageManager();

        double delta = 0.1;
        int outBid = event.getMyBid(); //TODO: Add delta

        if (!enable_app_combo) {
            Iterator<String> clusterIDIterator = storageManager.getTable("cluster_table").primaryKeyIterator();

            //Iterate through all clusterIDs
            while (clusterIDIterator.hasNext()) {
                String clusterID = clusterIDIterator.next();

                LOG.info("Posting event: " + event.getMyBid());

                ESEvent outEvent = new ESEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), clusterID);
                GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent);
                Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

                collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method
            }

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getMyBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

    protected void insertMaker(CUEvent event) throws InterruptedException {

        double delta = 0.1;
        int outBid = event.getMyBid(); //TODO: Add delta

        //sourceID is used in ???, myIteration is used in SStore
        Tuple marker = new Tuple(outBid, 0, context, new Marker(DEFAULT_STREAM_ID, -1, outBid, 0));

        LOG.info("Inserting marker: " + event.getMyBid());

        if (!enable_app_combo) {
            collector.emit(outBid, marker);//tuple should be the input of next bolt's execute() method
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, "Maker", event.getTimestamp())));
            }
        }
    }

}
