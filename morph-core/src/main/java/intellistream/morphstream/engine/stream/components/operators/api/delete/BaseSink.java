package intellistream.morphstream.engine.stream.components.operators.api.delete;

import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.formatter.BasicFormatter;
import intellistream.morphstream.engine.stream.components.formatter.Formatter;
import intellistream.morphstream.engine.stream.components.operators.base.unionBolt;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.util.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class BaseSink extends unionBolt {
    protected static final int max_num_msg = (int) 1E5;
    protected static final int skip_msg = 0;
    protected static final long[] latency_map = new long[max_num_msg];
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);
    private static final long serialVersionUID = 2236353891713886536L;
    public static ExecutionGraph graph;
    protected int thisTaskId;
    protected boolean isSINK = false;

    protected BaseSink(Logger log) {
        super(log);
    }

    BaseSink(Map<String, Double> input_selectivity, double read_selectivity) {
        super(LOG, input_selectivity, null, 1, read_selectivity);
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
        return String.format(BaseConstants.BaseConf.SINK_FORMATTER, configPrefix);
    }

    protected abstract Logger getLogger();

    protected void killTopology() {
        if (CONTROL.enable_log) LOG.info("Killing application");
//		System.exit(0);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
}
