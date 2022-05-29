package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import components.operators.api.TransactionalBolt;
import db.DatabaseException;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_latency_measurement;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class TRBolt extends TransactionalBolt {

    SINKCombo sink; // the default "next bolt"
    String[] words = {"Apple", "Banana", "Orange", "Grape"}; // assume there are words in the input tweet
    int tweetID = 1000; // assume this is the tweetID after the initial WRITE(new tweet)
    Tuple tuple;

    public TRBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "trtxn"; // TODO: Register this bolt in Config
    }

    // Method used to compute the output value, here it's a tuple (word, tweetID)
    List<Object> getOutputTuple(String word) {
        List<Object> outputTuple = new ArrayList<Object>();
        outputTuple.add(word);
        outputTuple.add(tweetID); // TODO: Used default tweetID, need to replace with the actual registered tweetID
        return outputTuple;
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    // Method for post-process
    void REQUEST_POST(TREvent event) throws InterruptedException {
        // Things to be pushed to the next bolt: (word, tweetID)
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            // for word in tweet: push the tuples (word, tweetID) to the next bolt
            for (String word : words) {
                List<Object> outputTuple = getOutputTuple(word);
                if (enable_latency_measurement)
                  tuple = new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, outputTuple, event.getTimestamp()));
              else
                  tuple = null;
              sink.execute(tuple); // TODO: Replace Sink with the next bolt
            }
        }
    }

    @Override
    protected void POST_PROCESS(long bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TREvent event = (TREvent) input_event;
            ((TREvent) input_event).setTimestamp(timestamp);
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

}
