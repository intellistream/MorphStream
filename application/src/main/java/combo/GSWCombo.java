package combo;

import benchmark.DataHolder;
import common.bolts.transactional.gsw.*;
import common.collections.Configuration;
import common.param.TxnEvent;
import common.param.mb.MicroEvent;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static common.CONTROL.*;
import static content.Content.*;

public class GSWCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(GSWCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{40};
    int cnt = 0;
    ArrayDeque<MicroEvent> prevents = new ArrayDeque<>();

    public GSWCombo() {
        super(LOG, 0);
    }

    @Override
    public void loadEvent(String filePath, Configuration config, TopologyContext context, OutputCollector collector) {
        int storageIndex = 0;
        //Load Transfer Events.
        for (int index = taskId; index < DataHolder.events.size(); ) {
            TxnEvent event = DataHolder.events.get(index).cloneEvent();
            mybids[storageIndex] = event.getBid();
            myevents[storageIndex++] = event;
            if (storageIndex == num_events_per_thread)
                break;
            index += tthread * combo_bid_size;
        }

//        //Load Deposit Events.
//        for (int index = taskId; index < DataHolder.depositEvents.size(); ) {
//            TxnEvent event = DataHolder.depositEvents.get(index).cloneEvent();
//            mybids[storageIndex] = event.getBid();
//            myevents[storageIndex++] = event;
//            if (storageIndex == num_events_per_thread)
//                break;
//            index += tthread * combo_bid_size;
//        }
        assert (storageIndex == num_events_per_thread);
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
                bolt = new GSWBolt_nocc(0, sink);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new GSWBolt_olb(0, sink);
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new GSWBolt_lwm(0, sink);
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new GSWBolt_ts(0, sink);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new GSWBolt_sstore(0, sink);
                break;
            }

            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        bolt.loadDB(config, context, collector);
        loadEvent(config.getString("rootFilePath"), config, context, collector);
//        bolt.sink.batch_number_per_wm = batch_number_per_wm;
    }
}