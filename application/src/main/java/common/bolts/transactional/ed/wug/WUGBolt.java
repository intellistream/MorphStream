package common.bolts.transactional.ed.wug;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import common.param.ed.wu.WUEvent;
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

public abstract class WUGBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"

    public WUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_wug"; // TODO: Register this bolt in Config
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void WU_GATE_REQUEST_POST(WUEvent event) throws InterruptedException, DatabaseException {

        StorageManager storageManager = db.getStorageManager();

        double delta = 0.1;
        int outBid = event.getMyBid(); //TODO: Add delta

        if (!enable_app_combo) {
            Iterator<String> wordIDIterator = storageManager.getTable("word_table").primaryKeyIterator();

            //Iterate through all wordIDs
            while (wordIDIterator.hasNext()) {
                String wordID = wordIDIterator.next();

                TCEvent outEvent = new TCEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), wordID);
                GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent);
                Tuple tuple = new Tuple(outBid, 0, context, generalMsg);

                LOG.info("Posting event: " + event.getMyBid());

                collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method
            }

        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getMyBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

    protected void insertMaker(WUEvent event) throws InterruptedException {

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
