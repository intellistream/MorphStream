package common.sink;
import common.Constants;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.datatype.util.LRTopologyControl;
import common.sink.helper.stable_sink_helper;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.operators.api.BaseSink;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Tuple;

import java.io.*;
import java.util.HashMap;

import static common.Constants.System_Plan_Path;
public class MeasureSink_latency extends BaseSink {
    protected static final Logger LOG = LoggerFactory.getLogger(MeasureSink_latency.class);
    protected static final DescriptiveStatistics latency = new DescriptiveStatistics();
    //	private static final HashMap<Long, Long> latency_map = new HashMap<>();
    protected static final int max_num_msg = (int) 1E5;
    protected static final int skip_msg = 0;
    protected static final long[] latency_map = new long[max_num_msg];
    private static final long serialVersionUID = 6249684803036342603L;
    protected static int num_msg = 0;
    protected static String directory;
    protected static String metric_path;
    protected static String algorithm;
    protected static boolean profile = false;
    protected stable_sink_helper helper;
    protected int cnt = 0;
    int sink_ID;
    public MeasureSink_latency() {
        super(new HashMap<>());
        this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
    }
    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int size = graph.getSink().operator.getExecutorList().size();
        metric_path = config.getString("metrics.output")
//				+ "/"
//				+ config.getInt("num_socket")
//				+ "_" + String.valueOf(config.getInt("tthread"))
        ;
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , metric_path, config.getDouble("predict", 0), size, thread_Id, false);
        common.sink.helper.helper helper2 = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , metric_path, config.getDouble("predict", 0), size, thread_Id, false);
        profile = config.getBoolean("profile");
        directory = System_Plan_Path + OsUtils.OS_wrapper("sesame")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")))
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("gc_factor")))
        ;
        File file = new File(directory);
        if (!file.mkdirs()) {
        }
        if (config.getBoolean("random", false)) {
            algorithm = "random";
        } else if (config.getBoolean("toff", false)) {
            algorithm = "toff";
        } else if (config.getBoolean("roundrobin", false)) {
            algorithm = "roundrobin";
        } else if (config.getBoolean("worst", false)) {
            algorithm = "worst";
        } else {
            algorithm = "opt";
        }
//		store = new ArrayDeque<>((int) 1E11);
        sink_ID = graph.getSink().getExecutorID();
        if (thisTaskId == sink_ID) {
            isSINK = true;
        }
    }
    @Override
    public void execute(Tuple input) {
        double results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            check();
        }
    }
    @Override
    public void execute(JumboTuple input) {
        //	store.add(input);
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
//			read = (input.getString(0, i));
//          simulate work..
//			dummy_execute();
            double results = helper.execute(input.getBID());
            if (isSINK && cnt % 1E3 == 0) {
                long msgId = input.getLong(0, i);
                if (msgId != -1) {
                    final long end = System.nanoTime();
                    final long start = input.getLong(1, i);
                    final long process_latency = end - start;//ns
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
                    //LOG.DEBUG("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                    latency_map[(int) msgId] = process_latency;
//				}
                }
                if (results != 0) {
                    this.setResults(results);
                    LOG.info("Sink finished:" + results);
                    check();
                }
            }
        }
    }
    /**
     * Only one sink will do the measure_end.
     */
    protected void check() {
        if (!profile) {
            for (int key = 0; key < num_msg; key++) {
//                LOG.info("=====Process latency of msg====");
                latency.addValue((latency_map[key] / 1E6));
            }
            try {
//                Collections.sort(col_value);
                FileWriter f = null;
                switch (algorithm) {
                    case "random": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.latency")));
                        break;
                    }
                    case "toff": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("toff.latency")));
                        break;
                    }
                    case "roundrobin": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("roundrobin.latency")));
                        break;
                    }
                    case "worst": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.latency")));
                        break;
                    }
                    case "opt": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.latency")));
                        break;
                    }
                    default:
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("latency")));
                        break;
                }
                Writer w = new BufferedWriter(f);
                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(String.valueOf(latency.getPercentile(percentile) + "\n"));
                }
                w.write("=======Details=======");
                w.write(latency.toString() + "\n");
                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("Stop all threads sequentially");
//			context.stop_runningALL();
            context.Sequential_stopAll();
//			try {
//				//Thread.sleep(10000);
//				context.wait_for_all();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			context.force_existALL();
//			context.stop_running();
//			try {
//				Thread.sleep(10000);//sync_ratio for all sink threads stop.
//			} catch (InterruptedException e) {
//				//e.printStackTrace();
//			}
        }
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
