package sesame.components.operators.api;

import application.constants.BaseConstants.BaseConf;
import sesame.components.formatter.BasicFormatter;
import sesame.components.formatter.Formatter;
import application.util.ClassLoaderUtils;
import application.util.Configuration;
import sesame.components.operators.base.unionBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public abstract class BaseSink extends unionBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);
    private static final long serialVersionUID = 2236353891713886536L;
    public static ExecutionGraph graph;
    protected int thisTaskId;
    protected boolean isSINK = false;
    protected static final int max_num_msg = (int) 1E5;
    protected static final int skip_msg = 0;
    protected BaseSink(Logger log) {
        super(log);
    }

    protected static final long[] latency_map = new long[max_num_msg];
    BaseSink(Map<String, Double> input_selectivity, double read_selectivity) {
        super(LOG, input_selectivity, null, (double) 1, read_selectivity);
    }

    protected BaseSink(Map<String, Double> input_selectivity) {
        this(input_selectivity, 0);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thisTaskId = thisTaskId;
        BaseSink.graph = graph;
        String formatterClass = config.getString(getConfigKey(), null);

        Formatter formatter;
        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(Configuration.fromMap(config), getContext());

        if (thisTaskId == graph.getSink().getExecutorID()) {
            isSINK = true;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }

    private String getConfigKey() {
        return String.format(BaseConf.SINK_FORMATTER, configPrefix);
    }

    protected abstract Logger getLogger();

    protected void killTopology() {
        LOG.info("Killing application");
//		System.exit(0);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
}
