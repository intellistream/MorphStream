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

import static application.datatype.util.VSTopologyControl.ECR24_STREAM_ID;
import static application.datatype.util.VSTopologyControl.ECR_STREAM_ID;
import static application.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ECRBolt.class);
    private static final long serialVersionUID = -5143351909038517995L;
    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public ECRBolt(String configPrefix) {
        super(configPrefix, null, new HashMap<>(), 0.12);
        if (configPrefix.equalsIgnoreCase("ecr24")) {
            this.output_selectivity.put(ECR24_STREAM_ID, 0.936305488817849);
        } else {
            this.output_selectivity.put(ECR_STREAM_ID, 0.936305488817849);
        }
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);

        if (configPrefix.equalsIgnoreCase("ecr24")) {
            streams.put(ECR24_STREAM_ID, fields);
        } else {
            streams.put(ECR_STREAM_ID, fields);
        }

        return streams;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //        if (stat != null) stat.start_measure();
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
//        cnt++;
            if (cdr.isCallEstablished()) {
                String caller = cdr.getCallingNumber();
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                // add numbers to filters
                filter.add(caller, 1, timestamp);
                double ecr = filter.estimateCount(caller, timestamp);


                if (configPrefix.equalsIgnoreCase("ecr24")) {

                    collector.emit(ECR24_STREAM_ID, bid, new StreamValues(caller, timestamp, ecr, cdr));

                } else {

                    collector.emit(ECR_STREAM_ID, bid, new StreamValues(caller, timestamp, ecr, cdr));

                }
//            cnt1++;
            }
//        double i = cnt1 / cnt;
//        if (stat != null) stat.end_measure();
        }


    }

    public void display() {

//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }
}
