package intellistream.morphstream.examples.tsp.onlinebiding.op;

import intellistream.morphstream.engine.txn.DataHolder;
import intellistream.morphstream.examples.utils.SPOUTCombo;
import intellistream.morphstream.examples.tsp.onlinebiding.events.AlertTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.ToppingTxnEvent;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.txn.TxnEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static intellistream.morphstream.configuration.CONTROL.combo_bid_size;
import static intellistream.morphstream.configuration.Constants.*;

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
        if (event instanceof AlertTxnEvent)
            keys = ((AlertTxnEvent) event).getItemId();
        else if (event instanceof ToppingTxnEvent)
            keys = ((ToppingTxnEvent) event).getItemId();
        else
            keys = ((BuyingTxnEvent) event).getItemId();
        return keys;
    }

    private int getLength(TxnEvent event) {
        if (event instanceof AlertTxnEvent)
            return ((AlertTxnEvent) event).getItemId().length;
        else if (event instanceof ToppingTxnEvent)
            return ((ToppingTxnEvent) event).getItemId().length;
        else
            return ((BuyingTxnEvent) event).getItemId().length;
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
        int storageIndex = 0;
        //Load OnlineBiding Events.
        for (int index = taskId; index < DataHolder.txnEvents.size(); ) {
            TxnEvent event = DataHolder.txnEvents.get(index).cloneEvent();
            mybids[storageIndex] = event.getBid();
            myevents[storageIndex++] = event;
            if (storageIndex == num_events_per_thread)
                break;
            index += tthread * combo_bid_size;
        }
        assert (storageIndex == num_events_per_thread);
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
            case CCOption_MorphStream: {//T-Stream
                bolt = new OBBolt_ts_s(0, sink);
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
        bolt.loadDB(config, context, collector);
        loadEvent(config.getString("rootFilePath"), config, context, collector);
//        bolt.sink.batch_number_per_wm = batch_number_per_wm;
    }
}