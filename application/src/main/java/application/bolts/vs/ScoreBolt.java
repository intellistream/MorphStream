package application.bolts.vs;

import application.bolts.comm.AbstractScoreBolt;
import application.datatype.util.VSTopologyControl;
import application.constants.VoIPSTREAMConstants;
import application.model.cdr.CallDetailRecord;
import application.util.datatypes.StreamValues;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static application.constants.VoIPSTREAMConstants.Conf;
import static application.constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ScoreBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreBolt.class);
    private static final long serialVersionUID = 2301076847738618548L;
    //    private double cnt = 0, cnt1 = 0, cnt2 = 0, cnt3 = 0;
    private double[] weights;

    public ScoreBolt() {
        super(LOG, null, new HashMap<>(), null, 0.002);
        this.input_selectivity.put(VSTopologyControl.FoFIR_STREAM_ID, 1.0);//0.552
        this.input_selectivity.put(VSTopologyControl.URL_STREAM_ID, 0.0);//0.552
        this.input_selectivity.put(VSTopologyControl.ACD_STREAM_ID, 0.0);//0.552
    }

    /**
     * Computes weighted sum of a given sequence.
     *
     * @param data    data array
     * @param weights weights
     * @return weighted sum of the data
     */
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i = 0; i < data.length; i++) {
            sum += (data[i] * weights[i]);
        }

        return sum;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(VoIPSTREAMConstants.Field.CALLING_NUM, VoIPSTREAMConstants.Field.TIMESTAMP
                , VoIPSTREAMConstants.Field.SCORE, VoIPSTREAMConstants.Field.RECORD);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);
        // parameters
        double fofirWeight = config.getDouble(Conf.FOFIR_WEIGHT);
        double urlWeight = config.getDouble(Conf.URL_WEIGHT);
        double acdWeight = config.getDouble(Conf.ACD_WEIGHT);

        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
        Source src = parseComponentId(in.getSourceComponent());
        String caller = cdr.getCallingNumber();
        long timestamp = cdr.getAnswerTime().getMillis() / 1000;
        double score = in.getDouble(2);
        String key = String.format("%s:%d", caller, timestamp);

        update_map(bid, cdr, src, caller, timestamp, score, key);


//        double i = cnt1 / cnt;
//        if (stat != null) stat.end_measure();

//        if (cnt % 10000 == 0)
//            LOG.info("ScoreBolt received: " + cnt + "(" + ((cnt1 + cnt2 + cnt3) / cnt) + ")" + " tuple"
//                    + "\tand emit1:" + cnt1 + "(" + (cnt1 / cnt) + ")" + "\temit2:" + cnt2 + "(" + (cnt2 / cnt) + ")" + "\temit3:" + cnt3 + "(" + (cnt3 / cnt) + ")");

    }

    private void update_map(long bid, CallDetailRecord cdr, Source src, String caller, long timestamp, double score, String key) throws InterruptedException {
        if (map.containsKey(key)) {
            Entry e = map.get(key);

            if (e.isFull()) {
                double mainScore = sum(e.getValues(), weights);

                ////LOG.DEBUG(String.format("Score=%f; Scores=%s", mainScore, Arrays.show(e.getMsg())));
//                cnt1++;
                collector.emit(bid, new StreamValues(caller, timestamp, mainScore, cdr));//0.56%
            } else {
//                cnt3++;
                e.set(src, score);
//                cnt1++;
                collector.emit(bid, new StreamValues(caller, timestamp, 0, cdr));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, score);
            map.put(key, e);
//            cnt2++;
//            cnt1++;
            collector.emit(bid, new StreamValues(caller, timestamp, 0, cdr));//3.64%
        }
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
            Source src = parseComponentId(in.getSourceComponent());
            String caller = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;
            double score = in.getDouble(2, i);
            String key = String.format("%s:%d", caller, timestamp);

            update_map(bid, cdr, src, caller, timestamp, score, key);
        }

    }

    //    public void display() {
////        LOG.info("ScoreBolt received: " + cnt + "(" + ((cnt1 + cnt2 + cnt3) / cnt) + ")" + " tuple"
////                + "\tand emit1:" + cnt1 + "(" + (cnt1 / cnt) + ")" + "\temit2:" + cnt2 + "(" + (cnt2 / cnt) + ")" + "\temit3:" + cnt3 + "(" + (cnt3 / cnt) + ")");
//    }
//    public void display() {
//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
//    }
    @Override
    protected Source[] getFields() {
        return new Source[]{Source.FOFIR, Source.URL, Source.ACD};
    }
}
