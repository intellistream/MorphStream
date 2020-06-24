package application.bolts.vs;

import application.bolts.comm.AbstractFilterBolt;
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

import static application.datatype.util.VSTopologyControl.RCR_STREAM_ID;
import static application.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class RCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RCRBolt.class);
    private static final long serialVersionUID = 6746918759145533839L;
    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public RCRBolt() {
        super("rcr", new HashMap<>(), new HashMap<>(), 0.01);
        this.output_selectivity.put(RCR_STREAM_ID, 0.4681533769629575);//only half of the input stream generates data.., which has a selectivity of 0.97
        this.input_selectivity.put(VoIPSTREAMConstants.Stream.BACKUP, 1.0);//only half of the input stream generates data.., which has a selectivity of 0.97
        this.input_selectivity.put(VoIPSTREAMConstants.Stream.DEFAULT, 1.0);//only half of the input stream generates data.., which has a selectivity of 0.97

    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);
        streams.put(RCR_STREAM_ID, fields);
        return streams;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);

        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            if (in.getSourceStreamId().equals(VoIPSTREAMConstants.Stream.DEFAULT)) {
                String callee = cdr.getCalledNumber();
                filter.add(callee, 1, timestamp);
//
            } else if (in.getSourceStreamId().equals(VoIPSTREAMConstants.Stream.BACKUP)) {
                String caller = cdr.getCallingNumber();
                double rcr = filter.estimateCount(caller, timestamp);
//                cnt1++;


                collector.emit(RCR_STREAM_ID, bid, new StreamValues(caller, timestamp, rcr, cdr));

            }
        }
//        if (stat != null) stat.end_measure();

    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //        cnt++;
//        if (stat != null) stat.start_measure();
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);

            if (cdr.isCallEstablished()) {
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                if (in.getSourceStreamId(i).equals(VoIPSTREAMConstants.Stream.DEFAULT)) {
                    String callee = cdr.getCalledNumber();
                    filter.add(callee, 1, timestamp);
//
                } else if (in.getSourceStreamId(i).equals(VoIPSTREAMConstants.Stream.BACKUP)) {
                    String caller = cdr.getCallingNumber();
                    double rcr = filter.estimateCount(caller, timestamp);
//                cnt1++;
                    collector.emit(RCR_STREAM_ID, bid, new StreamValues(caller, timestamp, rcr, cdr));

                }
            }
//        if (stat != null) stat.end_measure();

        }
    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }
}
