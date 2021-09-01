package components.operators.api;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import components.context.TopologyContext;
import db.Database;
import execution.ExecutionGraph;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.OutputFieldsDeclarer;
import lock.OrderLock;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import transaction.context.TxnContext;

import java.util.HashMap;
import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_log;
import static common.Constants.DEFAULT_STREAM_ID;
import static common.constants.BaseConstants.BaseField.TEXT;

public abstract class Operator implements IOperator {
    private static final long serialVersionUID = -7816511217365808709L;
    /**
     * Because the flexibility of noSQL stream processing, we force user to tell us the output formulation.
     */
    //TODO: This is not a complete list.
    public static final String map = "map";//Takes one element and produces one element. A map function that doubles the values of the input stream
    public static final String filter = "filter";//Evaluates a boolean function for each element and retains those for which the function returns true, e.g., A filter that filters out zero values:
    public static final String reduce = "reduce";//Combine multiple input data into one output data.
    public static final String w_apply = "w_apply";
    public final Map<String, Double> input_selectivity;//input_selectivity used to capture multi-stream effect.
    public final Map<String, Double> output_selectivity;//output_selectivity can be > 1
    public final double branch_selectivity;
    protected final Map<String, Fields> fields;
    public double read_selectivity;//the ratio of actual reading..
    public boolean scalable = true;
    public TopologyContext context;
    public transient Database db;//this is only used if the bolt is transactional bolt. DB is shared by all operators.
    //    public transient TxnContext txn_context;
    public transient TxnContext[] txn_context = new TxnContext[combo_bid_size];
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
    Operator(Logger log, Map<String, Double> input_selectivity,
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

    Operator(Logger log, double w) {
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

    public void setStateful() {
        Stateful = true;
    }

    public void display() {
    }

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
        return new Fields(TEXT);
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

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    public void loadDB(int thread_Id, ExecutionGraph graph) {
        graph.topology.tableinitilizer.loadDB(thread_Id, this.context.getNUMTasks());
    }

    public void loadDB(SchedulerContext schedulerContext, int thread_Id, ExecutionGraph graph) {
        graph.topology.tableinitilizer.loadDB(schedulerContext, thread_Id, this.context.getNUMTasks());
    }

    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (enable_log) LOG.info("The operator" + executor.getOP() + "does not require initialization");
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
            if (enable_log) LOG.info("The operator has no LOG, creates a default one for it here.");
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        db = getContext().getDb();
        initialize(thread_Id, thisTaskId, graph);
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
