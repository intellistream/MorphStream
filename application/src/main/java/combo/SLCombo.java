package combo;
import common.bolts.transactional.sl.*;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static common.CONTROL.*;
import static state_engine.content.Content.*;
//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class SLCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(SLCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{1, 10, 40};
    int cnt = 0;
    ArrayDeque<TxnEvent> prevents = new ArrayDeque<>();
    public SLCombo() {
        super(LOG, 0);
    }
    private boolean key_conflict(int pre_key, int key) {
        return pre_key == key;
    }
    private int[] getKeys(TxnEvent event) {
        int[] keys;
        if (event instanceof DepositEvent)
            keys = new int[]{
                    Integer.parseInt(((DepositEvent) event).getAccountId()),
                    Integer.parseInt(((DepositEvent) event).getBookEntryId())
            };
        else
            keys = new int[]{
                    Integer.parseInt(((TransactionEvent) event).getSourceAccountId()),
                    Integer.parseInt(((TransactionEvent) event).getTargetAccountId()),
                    Integer.parseInt(((TransactionEvent) event).getSourceBookEntryId()),
                    Integer.parseInt(((TransactionEvent) event).getTargetBookEntryId())
            };
        return keys;
    }
    private int getLength(TxnEvent event) {
        if (event instanceof DepositEvent)
            return 2;
        return 4;
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

        String Synth_Data_Folder_Path = System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA");
        File file = new File(Synth_Data_Folder_Path+OsUtils.OS_wrapper("transactions.txt"));

        if (!file.exists())
            throw new UnsupportedOperationException();

        Scanner sc;
        try {

            Object event = null;
            sc = new Scanner(file);
            for (int j = 0; j < taskId; j++) {
                sc.nextLine();
            }

            int index = 0;
            while (sc.hasNextLine()) {
                String read = sc.nextLine();
                String[] split = read.split(",");
                event = new TransactionEvent(
                        Integer.parseInt(split[0]), //bid
                        0, //pid
                       String.format("[%s]", split[0]), //bid_array
                        1,//num_of_partition
                        split[1],//getSourceAccountId
                        split[2],//getSourceBookEntryId
                        split[3],//getTargetAccountId
                        split[4],//getTargetBookEntryId
                        100,  //getAccountTransfer
                        100  //getBookEntryTransfer
                );
//                db.eventManager.put(input_event, Integer.parseInt(split[0]));

                mybids[index] = Integer.parseInt(split[0]);
                myevents[index++] = event;
                if (index  == num_events_per_thread) {
                    break;
                }
                if(index%batch_number_per_wm==0) {
                    for (int j = 0; j < (totalEventsPerBatch+taskId-1-(taskId+(batch_number_per_wm-1)*tthread)); j++) {
                        if (sc.hasNextLine())
                            sc.nextLine();//skip un-related.
                    }
                } else {
                    for (int j = 0; j < (tthread - 1) * combo_bid_size; j++) {
                        if (sc.hasNextLine())
                            sc.nextLine();//skip un-related.
                    }
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
                bolt = new SLBolt_nocc(0, sink);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new SLBolt_olb(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new SLBolt_lwm(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                if (config.getBoolean("disable_pushdown", false))
                    bolt = new SLBolt_ts_nopush(0, sink);
                else
                    bolt = new SLBolt_ts(0, sink);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new SLBolt_sstore(0, sink);
                _combo_bid_size = 1;
                break;
            }
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);
        loadEvent("transactions.txt", config, context, collector);
//        loadEvent("SL_Events" + tthread, config, context, collector);
//        bolt.sink.batch_number_per_wm = batch_number_per_wm;
    }
}