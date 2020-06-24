package application.bolts.vs;

import application.model.cdr.CallDetailRecord;
import application.util.OsUtils;
import application.util.datatypes.StreamValues;
import application.util.hash.BloomFilter;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

import static application.constants.VoIPSTREAMConstants.*;

public class VoiceDispatcherBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VoiceDispatcherBolt.class);
    private static final long serialVersionUID = 4553221616346819685L;
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private double cycleThreshold;

    public VoiceDispatcherBolt() {
        super(LOG, 0.08);//output two streams.
//        this.output_selectivity.put(Stream.DEFAULT, 1.0);
        this.output_selectivity.put(Stream.BACKUP, 1.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.NEW_CALLEE, Field.RECORD);

        streams.put(Stream.DEFAULT, fields);
        streams.put(Stream.BACKUP, fields);

        return streams;
    }

    private void VD_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        BufferedWriter writer;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("vd_threadId.txt")));
            writer = new BufferedWriter(fw);
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int approxInsertSize = config.getInt(Conf.VAR_DETECT_APROX_SIZE);
        double falsePostiveRate = config.getDouble(Conf.VAR_DETECT_ERROR_RATE);

        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);

        cycleThreshold = detector.size() / Math.sqrt(2);

        // initMetrics(context);
        //VD_pid();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
        String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // measure_end if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        StreamValues streamValues = new StreamValues(cdr.getCallingNumber(), cdr.getCalledNumber(),
                cdr.getAnswerTime(), newCallee, cdr);

        collector.emit(bid, streamValues);

        collector.emit(Stream.BACKUP, bid, streamValues);
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
            String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
            boolean newCallee = false;

            // add pair to learner
            learner.add(key);

            // measure_end if the pair exists
            // if not, add to the detector
            if (!detector.membershipTest(key)) {
                detector.add(key);
                newCallee = true;
            }

            // if number of non-zero bits is above threshold, rotate filters
            if (detector.getNumNonZero() > cycleThreshold) {
                rotateFilters();
            }

            StreamValues streamValues = new StreamValues(cdr.getCallingNumber(), cdr.getCalledNumber(),
                    cdr.getAnswerTime(), newCallee, cdr);

            collector.emit(bid, streamValues);

            collector.emit(Stream.BACKUP, bid, streamValues);
        }

    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}