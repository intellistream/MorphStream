package common.bolts.transactional.ed.cug;

import combo.SINKCombo;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import org.slf4j.Logger;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;

public abstract class CUGBolt extends TransactionalBolt {
    SINKCombo sink; // the default "next bolt"
    int clusterTableSize;

    public CUGBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ed_cug"; // TODO: Register this bolt in Config
        clusterTableSize = config.getInt("NUM_ITEMS", 0);
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
    }

    //Only called once after punctuation is reached
    protected void CU_GATE_REQUEST_POST(double starting_bid) throws InterruptedException, DatabaseException {

        double delta = 0.1;
        double outBid = starting_bid + delta;

        if (!enable_app_combo) {

            //Iterate through all clusterIDs
            for (int i = 0; i < clusterTableSize; i++) {
                LOG.info("Posting event: " + outBid);

                String clusterID = String.valueOf(i);

                //TODO: Pass event info from input CUEvent to output ESEvent

//                ESEvent outEvent = new ESEvent(outBid, event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(), event.getMyNumberOfPartitions(), clusterID);
//                GeneralMsg generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, outEvent);
//                Tuple tuple = new Tuple(outBid, 0, context, generalMsg);
//
//                collector.emit(outBid, tuple);//tuple should be the input of next bolt's execute() method

                outBid++;
            }

        } else {
            if (enable_latency_measurement) {
//                sink.execute(new Tuple(event.getMyBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.getTweetID(), event.getTimestamp())));
            }
        }
    }

}
