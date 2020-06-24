package application.bolts.comm;

import application.util.hash.ODTDBloomFilter;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static application.constants.VoIPSTREAMConstants.Conf;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFilterBolt.class);
    private static final long serialVersionUID = -3678935142534984021L;
    protected final String configPrefix;
    protected final String outputField;
    protected final String outputkeyField;
    protected ODTDBloomFilter filter;

    private AbstractFilterBolt(String configPrefix, Map<String, Double> output_selectivity, String outputField, String outputkeyField) {
        this(configPrefix, null, output_selectivity, outputField, outputkeyField);
    }

    private AbstractFilterBolt(String configPrefix, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, String outputField, String outputkeyField) {
        super(LOG, input_selectivity, output_selectivity);
        this.configPrefix = configPrefix;
        this.outputField = outputField;
        this.outputkeyField = outputkeyField;
    }

    protected AbstractFilterBolt(String configPrefix,
                                 Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double read_selectivity) {
        super(LOG, input_selectivity, output_selectivity, read_selectivity);
        this.configPrefix = configPrefix;
        this.outputField = application.constants.VoIPSTREAMConstants.Field.RATE;
        this.outputkeyField = null;
    }

//    @Override
//    public Fields getDefaultFields() {
//        if (this.outputkeyField == null)
//            return new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD);
//        else
//            return new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);
//    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        int numElements = config.getInt(String.format(Conf.FILTER_NUM_ELEMENTS, configPrefix));
        int bucketsPerElement = config.getInt(String.format(Conf.FILTER_BUCKETS_PEL, configPrefix));
        int bucketsPerWord = config.getInt(String.format(Conf.FILTER_BUCKETS_PWR, configPrefix));
        double beta = config.getDouble(String.format(Conf.FILTER_BETA, configPrefix));

        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }
}