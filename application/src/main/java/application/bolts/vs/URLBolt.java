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

import static application.datatype.util.VSTopologyControl.URL_STREAM_ID;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class URLBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(URLBolt.class);
    private static final long serialVersionUID = -2607881195007457873L;
    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public URLBolt() {
        super(LOG, "url", new HashMap<>(), new HashMap<>(), 0.002);
        this.input_selectivity.put(VSTopologyControl.ECR_STREAM_ID, 1.0);//0.552
        this.input_selectivity.put(VSTopologyControl.ENCR_STREAM_ID, 0.0);//0.552
        this.output_selectivity.put(URL_STREAM_ID, 0.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(VoIPSTREAMConstants.Field.CALLING_NUM, VoIPSTREAMConstants.Field.TIMESTAMP, VoIPSTREAMConstants.Field.SCORE, VoIPSTREAMConstants.Field.RECORD);
        streams.put(URL_STREAM_ID, fields);
        return streams;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValue(3);
        String number = in.getString(0);
        long timestamp = in.getLong(1);
        double rate = in.getDouble(2);

        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId(in.getSourceComponent());

        map_update(bid, cdr, number, timestamp, rate, key, src);
//        double i = cnt1 / cnt;
//        if (stat != null) stat.end_measure();
    }

    private void map_update(long bid, CallDetailRecord cdr, String number, long timestamp, double rate, String key, Source src) throws InterruptedException {
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);

            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ENCR) / e.get(Source.ECR));
                double score = score(thresholdMin, thresholdMax, ratio);

                ////LOG.DEBUG(String.format("T1=%f; T2=%f; ENCR=%f; ECR=%f; Ratio=%f; Score=%f",
                //        thresholdMin, thresholdMax, e.GetAndUpdate(Source.ENCR), e.GetAndUpdate(Source.ECR), ratio, score));
//                cnt1++;
                collector.emit(URL_STREAM_ID, bid, new StreamValues(number, timestamp, score, cdr));
                map.remove(key);
            } else {
                //LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                //       in.getSourceComponent(), e.show()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            CallDetailRecord cdr = (CallDetailRecord) in.getValue(3, i);
            String number = in.getString(0, i);
            long timestamp = in.getLong(1, i);
            double rate = in.getDouble(2, i);

            String key = String.format("%s:%d", number, timestamp);
            Source src = parseComponentId(in.getSourceComponent());

            map_update(bid, cdr, number, timestamp, rate, key, src);
        }
    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.ENCR, Source.ECR};
    }
}