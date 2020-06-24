package application.bolts.vs;

import application.bolts.comm.AbstractFilterBolt;
import application.model.cdr.CallDetailRecord;
import application.util.datatypes.StreamValues;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static application.datatype.util.VSTopologyControl.ENCR_STREAM_ID;
import static application.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user new callee rate
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ENCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ENCRBolt.class);
    private static final long serialVersionUID = -8766188120719118281L;

    //    private double cnt = 0, cnt1 = 0, cnt2 = 0;
    public ENCRBolt() {
        super("encr", null, new HashMap<>(), 0.002);
        this.output_selectivity.put(ENCR_STREAM_ID, 0.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);
        streams.put(ENCR_STREAM_ID, fields);
        return streams;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        final long bid = in.getBID();
//        if (stat != null) stat.start_measure();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
        boolean newCallee = in.getBooleanByField(Field.NEW_CALLEE);
//        cnt++;
        if (cdr.isCallEstablished() && newCallee) {
            String caller = in.getStringByField(Field.CALLING_NUM);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);
//            cnt1++;
            collector.emit(ENCR_STREAM_ID, bid, new StreamValues(caller, timestamp, rate, cdr));
        }
//        if (stat != null) stat.end_measure();
//        double i = cnt1 / cnt;
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //        if (stat != null) stat.start_measure();
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
            boolean newCallee = in.getBooleanByField(Field.NEW_CALLEE, i);
//        cnt++;
            if (cdr.isCallEstablished() && newCallee) {
                String caller = in.getStringByField(Field.CALLING_NUM, i);
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                filter.add(caller, 1, timestamp);
                double rate = filter.estimateCount(caller, timestamp);
//            cnt1++;
                collector.emit(ENCR_STREAM_ID, bid, new StreamValues(caller, timestamp, rate, cdr));
            }
//        if (stat != null) stat.end_measure();
//        double i = cnt1 / cnt;


        }
    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }
}