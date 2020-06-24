package application.bolts.comm;

import application.model.cdr.CallDetailRecord;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static application.constants.VoIPSTREAMConstants.Component;
import static application.constants.VoIPSTREAMConstants.Conf;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractScoreBolt extends filterBolt {
    private static final long serialVersionUID = 878583150054802430L;
    private final String configPrefix;
    protected double thresholdMin;
    protected double thresholdMax;
    protected Map<String, Entry> map;

    public AbstractScoreBolt(Logger LOG, String configPrefix, Map<String, Double> inputSelectivity, Map<String, Double> output_selectivity) {
        super(LOG, inputSelectivity, output_selectivity);
        this.configPrefix = configPrefix;
    }

    public AbstractScoreBolt(Logger LOG, String configPrefix, Map<String, Double> output_selectivity) {
        super(LOG, null, output_selectivity);
        this.configPrefix = configPrefix;
    }

    protected AbstractScoreBolt(Logger log, String configPrefix, Map<String, Double> inputSelectivity, Map<String, Double> output_selectivity, double read_selectivity) {
        super(log, inputSelectivity, output_selectivity, read_selectivity);
        this.configPrefix = configPrefix;
    }

    protected static Source parseComponentId(String id) {
        switch (id) {
            case Component.VOICE_DISPATCHER:
                return Source.VD;
            case Component.ECR24:
                return Source.ECR24;
            case Component.CT24:
                return Source.CT24;
            case Component.ECR:
                return Source.ECR;
            case Component.RCR:
                return Source.RCR;
            case Component.ENCR:
                return Source.ENCR;
            case Component.ACD:
                return Source.ACD;
            case Component.GLOBAL_ACD:
                return Source.GACD;
            case Component.URL:
                return Source.URL;
            case Component.FOFIR:
                return Source.FOFIR;
            default:
                return Source.NONE;
        }
    }

    protected static double score(double v1, double v2, double vi) {
        double score = vi / (v1 + (v2 - v1));
        if (score < 0) score = 0;
        if (score > 1) score = 1;
        return score;
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        map = new HashMap<>();//when this is too large..

        // parameters
        if (configPrefix != null) {
            thresholdMin = config.getDouble(String.format(Conf.SCORE_THRESHOLD_MIN, configPrefix));
            thresholdMax = config.getDouble(String.format(Conf.SCORE_THRESHOLD_MAX, configPrefix));
        }
    }

    protected abstract Source[] getFields();

    protected enum Source {
        ECR, RCR, ECR24, ENCR, CT24, VD, FOFIR, ACD, GACD, URL, NONE
    }

    protected class Entry {
        final CallDetailRecord cdr;

        final Source[] fields;
        final double[] values;

        public Entry(CallDetailRecord cdr) {
            this.cdr = cdr;
            this.fields = getFields();

            values = new double[fields.length];
            Arrays.fill(values, Double.NaN);
        }

        public void set(Source src, double rate) {
            values[pos(src)] = rate;
        }

        public double get(Source src) {
            return values[pos(src)];
        }

        public boolean isFull() {
            for (double value : values)
                if (Double.isNaN(value))
                    return false;
            return true;
        }

        private int pos(Source src) {
            for (int i = 0; i < fields.length; i++)
                if (fields[i] == src)
                    return i;
            return -1;
        }

        public double[] getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "Entry{" + "cdr=" + cdr + ", fields=" + Arrays.toString(fields) + ", values=" + Arrays.toString(values) + '}';
        }

    }
}