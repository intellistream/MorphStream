package combo;

import common.bolts.transactional.ob.*;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.ToppingEvent;
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

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class OBCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(OBCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{1, 10, 50, 100, 250, 500, 750, 1000};
    int cnt = 0;
    ArrayDeque<TxnEvent> prevents = new ArrayDeque<>();

    public OBCombo() {
        super(LOG, 0);
    }

    private boolean key_conflict(int pre_key, int key) {
        return pre_key == key;
    }

    private int[] getKeys(TxnEvent event) {
        int[] keys;
        if (event instanceof AlertEvent)
            keys = ((AlertEvent) event).getItemId();
        else if (event instanceof ToppingEvent)
            keys = ((ToppingEvent) event).getItemId();
        else
            keys = ((BuyingEvent) event).getItemId();
        return keys;
    }

    private int getLength(TxnEvent event) {
        if (event instanceof AlertEvent)
            return ((AlertEvent) event).getItemId().length;
        else if (event instanceof ToppingEvent)
            return ((ToppingEvent) event).getItemId().length;
        else
            return ((BuyingEvent) event).getItemId().length;
    }

    private int check_conflict(TxnEvent pre_event, TxnEvent event) {
        int conf = 0;//in case no conflict at all.
        for (int key : getKeys(event)) {
            int[] preEventKeys = getKeys(pre_event);
            for (int preEventKey : preEventKeys) {
                if (key_conflict(preEventKey, key))
                    conf++;
            }
        }
        return conf;
    }

    private int conflict(TxnEvent event) {
        int conc = getLength(event);//in case no conflict at all.
        for (TxnEvent prevent : prevents) {
            conc -= check_conflict(prevent, event);
        }
        return Math.max(0, conc);
    }

    protected void show_stats() {
        while (cnt < 8) {
            for (Object myevent : myevents) {
                concurrency += conflict((TxnEvent) myevent);
                prevents.add((TxnEvent) myevent);
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
    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition);
        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file_name))))
            System.exit(-1);
        Scanner sc;
        try {
            sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file_name)));
            int i = 0;
            Object event;
            for (int j = 0; j < taskId; j++) {
                sc.nextLine();
            }
            while (sc.hasNextLine()) {
                String read = sc.nextLine();
                String[] split = read.split(split_exp);
                if (split[4].endsWith("BuyingEvent")) {//BuyingEvent
                    event = new BuyingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], //bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            split[6],//price_array
                            split[7]  //qty_array
                    );
                } else if (split[4].endsWith("AlertEvent")) {//AlertEvent
                    event = new AlertEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], // bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7]//price_array
                    );
                } else {
                    event = new ToppingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], Integer.parseInt(split[1]), //pid
                            //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7]  //top_array
                    );
                }
//                db.eventManager.put(input_event, Integer.parseInt(split[0]));
                myevents[i++] = event;
                if (i == num_events_per_thread) break;
                for (int j = 0; j < (tthread - 1) * combo_bid_size; j++) {
                    if (sc.hasNextLine())
                        sc.nextLine();//skip un-related.
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (enable_debug)
            show_stats();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        _combo_bid_size = combo_bid_size;
        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new OBBolt_nocc(0, sink);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new OBBolt_olb(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new OBBolt_lwm(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new OBBolt_ts(0, sink);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new OBBolt_sstore(0, sink);
                _combo_bid_size = 1;
                break;
            }
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);
        loadEvent("OB_Events" + tthread, config, context, collector);
//        bolt.sink.batch_number_per_wm = batch_number_per_wm;
    }
}