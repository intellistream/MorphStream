package common.combo;

import common.bolts.transactional.gs.*;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.mb.MicroEvent;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Scanner;

import static common.CONTROL.*;
import static common.Constants.Event_Path;
import static content.Content.*;
import static profiler.Metrics.NUM_ACCESSES;
import static profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class GSCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(GSCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{40};
    int cnt = 0;
    ArrayDeque<MicroEvent> prevents = new ArrayDeque<>();

    public GSCombo() {
        super(LOG, 0);
    }

    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) throws FileNotFoundException {
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        int number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition)
                + OsUtils.OS_wrapper("NUM_EVENTS=" + config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + ratio_of_multi_partition)
                + OsUtils.OS_wrapper("number_partitions=" + number_partitions)
                + OsUtils.OS_wrapper("ratio_of_read=" + ratio_of_read)
                + OsUtils.OS_wrapper("NUM_ACCESSES=" + NUM_ACCESSES)
                + OsUtils.OS_wrapper("theta=" + config.getDouble("theta", 1))
                + OsUtils.OS_wrapper("NUM_ITEMS=" + NUM_ITEMS);
        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file_name))))
            throw new FileNotFoundException();
        long start = System.nanoTime();
        Scanner sc;
        int i = 0;
        try {
            sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file_name)));
            Object event = null;
            for (int j = 0; j < taskId; j++) {
                sc.nextLine();
            }
            while (sc.hasNextLine()) {
                String read = sc.nextLine();
                String[] split = read.split(split_exp);
                event = new MicroEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        Boolean.parseBoolean(split[6])//flag
                );
                myevents[i++] = event;
                if (i == num_events_per_thread) break;
                for (int j = 0; j < (tthread - 1) * combo_bid_size; j++) {
                    if (sc.hasNextLine())
                        sc.nextLine();//skip un-related.
                }
                //db.eventManager.put(input_event, Integer.parseInt(split[0]));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        LOG.info("Thread:" + taskId + " finished loading events (" + test_num_events_per_thread + ") in " + (System.nanoTime() - start) / 1E6 + " ms");
        if (enable_debug)
            show_stats();
    }

    private boolean key_conflict(int pre_key, int key) {
        return pre_key == key;
    }

    private int check_conflict(MicroEvent pre_event, MicroEvent event) {
        int conf = 0;//in case no conflict at all.
        for (int key : event.getKeys()) {
            int[] preEventKeys = pre_event.getKeys();
            for (int preEventKey : preEventKeys) {
                if (key_conflict(preEventKey, key))
                    conf++;
            }
        }
        return conf;
    }

    private int conflict(MicroEvent event) {
        int conc = 1;//in case no conflict at all.
        for (MicroEvent prevent : prevents) {
            conc -= check_conflict(prevent, event);
        }
        return Math.max(0, conc);
    }

    protected void show_stats() {
        while (cnt < 8) {
            for (Object myevent : myevents) {
                concurrency += conflict((MicroEvent) myevent);
                prevents.add((MicroEvent) myevent);
                if (prevents.size() == concerned_length[cnt]) {
                    if (pre_concurrency == 0)
                        pre_concurrency = concurrency;
                    else
                        pre_concurrency = (pre_concurrency + concurrency) / 2;
                    concurrency = 0;
                    prevents.clear();
                }
            }
            System.out.println(concerned_length[cnt] + ",\t " + pre_concurrency + ",");
            cnt++;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        assert enable_shared_state;//This application requires enable_shared_state.

        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new GSBolt_nocc(0, sink);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new GSBolt_olb(0, sink);
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new GSBolt_lwm(0, sink);
                break;
            }
            case CCOption_TStream: {//T-Stream
                    bolt = new GSBolt_ts(0, sink);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new GSBolt_sstore(0);
                break;
            }

            default:
                throw new UnsupportedOperationException("Please select correct CC option!");
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        bolt.loadDB(config, context, collector);
        try {
            loadEvent("GS_Events" + tthread, config, context, collector);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
//        bolt.sink.batch_number_per_wm = batch_number_per_wm;
    }
}