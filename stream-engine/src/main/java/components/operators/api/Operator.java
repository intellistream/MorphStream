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
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.OutputFieldsDeclarer;
import faulttolerance.State;
import lock.Clock;
import lock.OrderLock;
import lock.OrderValidate;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import transaction.context.TxnContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static common.constants.BaseConstants.BaseField.TEXT;

public abstract class Operator implements IOperator {
    /**
     * Because the flexibility of noSQL stream processing, we force user to tell us the output formulation.
     */
    //TODO: This is not a complete list.
    public static final String map = "map";//Takes one element and produces one element. A map function that doubles the values of the input stream
    public static final String filter = "filter";//Evaluates a boolean function for each element and retains those for which the function returns true, e.g., A filter that filters out zero values:
    public static final String reduce = "reduce";//Combine multiple input data into one output data.
    public static final String w_apply = "w_apply";
    private static final long serialVersionUID = -7816511217365808709L;
    public static String flatMap = "flatMap";//Takes one element and produces zero, one, or more elements, e.g., A flatmap function that splits sentences to words:
    public static String w_join = "w_join";//Join two data streams on a given key and a common window.
    public static String union = "union";//Union of two or more data streams creating a new stream containing all the elements from all the streams. SimExecutionNode: If you union a data stream with itself you will GetAndUpdate each element twice in the resulting stream.
    public final Map<String, Double> input_selectivity;//input_selectivity used to capture multi-stream effect.
    public final Map<String, Double> output_selectivity;//output_selectivity can be > 1
    public final double branch_selectivity;
    protected final Map<String, Fields> fields;
    private final boolean ByP;//Time by processing? or by input_event.
    private final double Event_frequency;
    public double read_selectivity;//the ratio of actual reading..
    public double loops = -1;//by default use argument loops.
    public boolean scalable = true;
    public TopologyContext context;
    public Clock clock;
    public State state = null;
    public transient Database db;//this is only used if the bolt is transactional bolt. DB is shared by all operators.
    //    public transient TxnContext txn_context;
    public transient TxnContext[] txn_context = new TxnContext[combo_bid_size];
    public boolean forceStop;
    public int fid = -1;//if fid is -1 it means it does not participate
    public OrderLock lock;//used for lock_ratio-based ordering constraint.
    public OrderValidate orderValidate;
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
     * @param byP
     * @param event_frequency
     * @param window_size
     */
    Operator(Logger log, Map<String, Double> input_selectivity,
             Map<String, Double> output_selectivity, double branch_selectivity,
             double read_selectivity, boolean byP, double event_frequency, double window_size) {
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
        ByP = byP;
        Event_frequency = event_frequency;
        window = window_size;
        fields = new HashMap<>();
    }

    Operator(Logger log, boolean byP, double event_frequency, double w) {
        LOG = log;
        this.input_selectivity = new HashMap<>();
        this.output_selectivity = new HashMap<>();
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.output_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.branch_selectivity = 1;
        this.read_selectivity = 1;
        ByP = byP;
        Event_frequency = event_frequency;
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

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
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
        if(enable_log) LOG.info("The operator" + executor.getOP() + "does not require initialization");
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
            if(enable_log) LOG.info("The operator has no LOG, creates a default one for it here.");
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        if (this instanceof Checkpointable) {
            if (state == null) {
                if(enable_log) LOG.info("The operator" + executor.getOP() + " is declared as checkpointable " +
                        "but no state is initialized");
            } else {
                if (!enable_app_combo) {
                    state.source_state_ini(executor);
                    state.dst_state_init(executor);
                }
            }
        }
        db = getContext().getDb();
        initialize(thread_Id, thisTaskId, graph);
    }

    public void setExecutionNode(ExecutionNode e) {
        this.executor = e;
    }

    /**
     * forward_checkpoint implementation
     * save state of the operator with or without MMIO.
     * TODO: support exactly once in future.
     *
     * @param value    the value_list to be updated.
     * @param sourceId
     * @param marker
     */
    public boolean checkpoint_store(Serializable value, int sourceId, Marker marker) {
        return state.share_store(value, sourceId, marker, executor, context.getThisComponentId() + context.getThisTaskId());
    }

    /**
     * Simple forward the marker
     *
     * @param sourceId
     * @return
     */
    public boolean checkpoint_forward(int sourceId) {
        return state.forward(sourceId, executor);
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
