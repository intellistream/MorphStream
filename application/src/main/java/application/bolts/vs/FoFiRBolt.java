package application.bolts.vs;

import application.bolts.comm.AbstractScoreBolt;
import application.datatype.util.VSTopologyControl;
import application.constants.VoIPSTREAMConstants;
import application.model.cdr.CallDetailRecord;
import application.util.datatypes.StreamValues;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static application.datatype.util.VSTopologyControl.FoFIR_STREAM_ID;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FoFiRBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FoFiRBolt.class);
    private static final long serialVersionUID = -321621602080096426L;

    //    private double cnt = 0, cnt1 = 0, cnt2 = 0;
    public FoFiRBolt() {
        super(LOG, "fofir", new HashMap<>(), new HashMap<>(), 0.2);
        this.input_selectivity.put(VSTopologyControl.RCR_STREAM_ID, 1.0);
        this.input_selectivity.put(VSTopologyControl.ECR_STREAM_ID, 1.0);
        this.output_selectivity.put(FoFIR_STREAM_ID, 1.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(VoIPSTREAMConstants.Field.CALLING_NUM, VoIPSTREAMConstants.Field.TIMESTAMP, VoIPSTREAMConstants.Field.SCORE, VoIPSTREAMConstants.Field.RECORD);
        streams.put(FoFIR_STREAM_ID, fields);
        return streams;
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.RCR, Source.ECR};
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {

        CallDetailRecord cdr = (CallDetailRecord) in.getValue(3);
        String number = in.getString(0);
        long timestamp = in.getLong(1);
        double rate = in.getDouble(2);
        final long bid = in.getBID();
        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId(in.getSourceComponent());

        update_map(cdr, number, timestamp, rate, bid, key, src);

    }

    private void update_map(CallDetailRecord cdr, String number, long timestamp, double rate, long bid, String key, Source src) throws InterruptedException {
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);

            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ECR) / e.get(Source.RCR));
                double score = score(thresholdMin, thresholdMax, ratio);


                collector.emit(FoFIR_STREAM_ID, bid, new StreamValues(number, timestamp, score, cdr));
                map.remove(key);
            } else {
                //LOG.DEBUG(String.format("Inconsistent entry: source=%s; %s",in.getSourceComponent(), e.toString()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

            CallDetailRecord cdr = (CallDetailRecord) in.getValue(3, i);
            String number = in.getString(0, i);
            long timestamp = in.getLong(1, i);
            double rate = in.getDouble(2, i);

            String key = String.format("%s:%d", number, timestamp);
            Source src = parseComponentId(in.getSourceComponent());

            update_map(cdr, number, timestamp, rate, bid, key, src);
        }


    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }
}