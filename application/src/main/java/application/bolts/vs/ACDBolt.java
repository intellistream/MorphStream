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

import static application.datatype.util.VSTopologyControl.ACD_STREAM_ID;
import static application.constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ACDBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ACDBolt.class);
    private static final long serialVersionUID = -4828169947581269578L;
    private double cnt = 0, cnt1 = 0, cnt2 = 0;
    private double avg;

    public ACDBolt() {
        super(LOG, "acd", new HashMap<>(), new HashMap<>(), 0.002);
        this.input_selectivity.put(VSTopologyControl.ECR24_STREAM_ID, 1.0);//0.552
        this.input_selectivity.put(VSTopologyControl.CTBolt_STREAM_ID, 1.0);//0.552
        this.input_selectivity.put(VSTopologyControl.GlobalACD_STREAM_ID, 1.0);//0.552
        this.output_selectivity.put(ACD_STREAM_ID, 0.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(VoIPSTREAMConstants.Field.CALLING_NUM, VoIPSTREAMConstants.Field.TIMESTAMP, VoIPSTREAMConstants.Field.SCORE, VoIPSTREAMConstants.Field.RECORD);
        streams.put(ACD_STREAM_ID, fields);
        return streams;
    }

    private void update(Source src, long bid, CallDetailRecord cdr, String number, long timestamp, double rate, String key) throws InterruptedException {
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);

            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.CT24) / e.get(Source.ECR24)) / avg;
                double score = score(thresholdMin, thresholdMax, ratio);

                LOG.debug(String.format("T1=%f; T2=%f; CT24=%f; ECR24=%f; AvgCallDur=%f; Ratio=%f; Score=%f", thresholdMin, thresholdMax, e.get(Source.CT24), e.get(Source.ECR24), avg, ratio, score));
//                    cnt1++;
                collector.emit(ACD_STREAM_ID, bid, new StreamValues(number, timestamp, score, cdr));
                map.remove(key);
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        Source src = parseComponentId(in.getSourceComponent());
        final long bid = in.getBID();
        if (src == Source.GACD) {
            avg = in.getDoubleByField(Field.AVERAGE);
        } else {
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
            String number = (String) in.getValueByField(Field.CALLING_NUM);
            long timestamp = (long) in.getValueByField(Field.TIMESTAMP);
            double rate = (double) in.getValueByField(Field.RATE);


            String key = String.format("%s:%d", number, timestamp);

            update(src, bid, cdr, number, timestamp, rate, key);
        }
//        if (stat != null) stat.end_measure();
//        double i = cnt1 / cnt;
    }



    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //        cnt++;
//        if (stat != null) stat.start_measure();
            Source src = parseComponentId(in.getSourceComponent());

            if (src == Source.GACD) {
                avg = in.getDoubleByField(Field.AVERAGE, i);
            } else {
                CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
                String number = (String) in.getValueByField(Field.CALLING_NUM, i);
                long timestamp = (long) in.getValueByField(Field.TIMESTAMP, i);
                double rate = (double) in.getValueByField(Field.RATE, i);


                String key = String.format("%s:%d", number, timestamp);

                update(src, bid, cdr, number, timestamp, rate, key);
            }

        }
    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.CT24, Source.ECR24};
    }
}