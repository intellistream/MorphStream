package intellistream.morphstream.engine.stream.components.operators.api;

import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.util.OsUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;

public abstract class Operator implements IOperator {
    private static final long serialVersionUID = -7816511217365808709L;
    public final Map<String, Double> input_selectivity;//input_selectivity used to capture multi-stream effect.
    public final Map<String, Double> output_selectivity;//output_selectivity can be > 1
    public final double branch_selectivity;
    protected final Map<String, Fields> fields;
    public double read_selectivity;//the ratio of actual reading..
    public boolean scalable = true;
    public TopologyContext context;
    public transient Database db;//this is only used if the bolt is transactional bolt. DB is shared by all operators.
    public transient TxnContext[] txn_context = new TxnContext[CONTROL.combo_bid_size];
    public int fid = -1;//if fid is -1 it means it does not participate
    public OrderLock lock;//used for lock_ratio-based ordering constraint.
    public String configPrefix = BaseConstants.BASE_PREFIX;
    protected OutputCollector collector;
    protected Configuration config;
    protected ExecutionNode executor;//who owns this Spout
    Logger LOG;
    boolean Stateful = false;
    private double window = 1;//by default window fieldSize is 1, means per-tuple execution
    private double results = 0;

    /**
     * @param log
     * @param output_selectivity
     * @param branch_selectivity
     * @param read_selectivity
     * @param window_size
     */
    public Operator(Logger log, Map<String, Double> input_selectivity,
                    Map<String, Double> output_selectivity, double branch_selectivity,
                    double read_selectivity, double window_size) {
        LOG = log;
        if (input_selectivity == null) {
            this.input_selectivity = new HashMap<>();
            this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        } else {
            this.input_selectivity = input_selectivity;
        }
        if (output_selectivity == null) {
            this.output_selectivity = new HashMap<>();
            this.output_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        } else {
            this.output_selectivity = output_selectivity;
        }
        this.branch_selectivity = branch_selectivity;
        this.read_selectivity = read_selectivity;

        window = window_size;
        fields = new HashMap<>();
    }

    public Operator(Logger log, double w) {
        LOG = log;
        this.input_selectivity = new HashMap<>();
        this.output_selectivity = new HashMap<>();
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.output_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.branch_selectivity = 1;
        this.read_selectivity = 1;

        window = w;
        fields = new HashMap<>();
    }
    /**
     * This is the API to talk to actual thread.
     *
     * @param conf
     * @param context
     * @param collector
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.config = Configuration.fromMap(conf);
        setContext(context);
        this.collector = collector;
        base_initialize(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }
    /**
     * Base init will always be called.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     */
    private void base_initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (LOG == null) {
            LOG = LoggerFactory.getLogger(Operator.class);
            if (CONTROL.enable_log) LOG.info("The operator has no LOG, creates a default one for it here.");
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        db = getContext().getDb();
        initialize(thread_Id, thisTaskId, graph);
    }
    public abstract void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph);
    public abstract void loadDB(Map conf, TopologyContext context, OutputCollector collector);
    public void setStateful() {
        Stateful = true;
    }

    public void display() {}

    public OutputCollector getCollector() {
        return collector;
    }

    public TopologyContext getContext() {
        return context;
    }

    private void setContext(TopologyContext context) {
        this.context = context;
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseConstants.BaseStream.DEFAULT, fields);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null) {
                fields.put(BaseConstants.BaseStream.DEFAULT, getDefaultFields());
            }
            if (getDefaultStreamFields() != null) {
                fields.putAll(getDefaultStreamFields());
            }
        }
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    /**
     * default field.
     *
     * @return
     */
    protected Fields getDefaultFields() {//@define the output fields
        return new Fields(BaseConstants.BaseField.TEXT);
    }

    protected Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    public String getConfigPrefix() {
        return this.configPrefix;
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    public int getId() {
        return this.executor.getExecutorID();
    }

    public double getWindow() {
        return window;
    }

    public void setWindow(double window) {
        this.window = window;
    }

    public double getResults() {
        return results;
    }

    public void setResults(double results) {
        this.results = results;
    }





    public void setExecutionNode(ExecutionNode e) {
        this.executor = e;
    }

    public Integer default_scale(Configuration conf) {
        return 1;
    }

    public int getFid() {
        return fid;
    }

    public double getEmpty() {
        return 0;
    }
}
