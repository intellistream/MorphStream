package combo;
import common.bolts.transactional.sl.*;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataConfig;
import common.topology.transactional.initializer.slinitializer.datagenerator.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Scanner;

import static common.CONTROL.*;
import static common.Constants.Event_Path;
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
                myevents[index ++] = event;
                if (index  == num_events_per_thread) break;
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

//    @Override
//    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {
////        tEvents = new Object[]
////        {
//////                new TransactionEvent(2,0, "[2]", 1, "2", "2", "4", "4", 2,  2  ),
////                new TransactionEvent(0,0, "[0]", 1, "2", "5", "1", "2", 2, 2),
////                new TransactionEvent(1,0, "[1]", 1, "3", "5", "6", "6", 2, 2),
////                new TransactionEvent(2,0, "[2]", 1, "7", "2", "3", "6", 2, 2),
////                new TransactionEvent(3,0, "[3]", 1, "2", "7", "3", "6", 2, 2),
//////                new TransactionEvent(3,0, "[3]", 1, "7", "6", "4", "5", 2, 2),
////                // to add dependency.
////                // Use the state as source with an operation registered on it.
////                // If used both ids which has op registered on them, will result in cross circular dependency
////                // This means we should use at least 1 new id in sources everytime.
////                //
////
////
//////                new TransactionEvent(0,0, "[0]", 1, "2", "5", "2", "2", 2,  2  ),
//////                    new TransactionEvent(7,0, "[7]", 1, "0", "0", "1", "1", 2,  2  ),
//////                    new TransactionEvent(8,0, "[8]", 1, "0", "0", "1", "1", 2,  2  ),
//////                    new TransactionEvent(9,0, "[9]", 1, "0", "0", "1", "1", 2,  2  )
////        };
//
//        int number_partitions = Math.min(tthread, config.getInt("number_partitions"));
//        String event_path = Event_Path
//                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition))
//                + OsUtils.OS_wrapper("NUM_EVENTS=" + String.valueOf(NUM_EVENTS))
//                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + String.valueOf(config.getDouble("ratio_of_multi_partition", 1)))
//                + OsUtils.OS_wrapper("number_partitions=" + String.valueOf(number_partitions));
//
//        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file_name))))
//            throw new UnsupportedOperationException();
//        Scanner sc;
//        try {
//            sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file_name)));
//            int i = 0;
//            Object event = null;
//            for (int j = 0; j < taskId; j++) {
//                sc.nextLine();
//            }
//            while (sc.hasNextLine()) {
//                String read = sc.nextLine();
//                String[] split = read.split(split_exp);
//                if (split.length < 4) {
//                    LOG.info("Loading wrong file!" + Arrays.toString(split));
//                    System.exit(-1);
//                }
//                if (split[4].endsWith("DepositEvent")) {//DepositEvent
//                    event = new DepositEvent(
//                            Integer.parseInt(split[0]), //bid
//                            Integer.parseInt(split[1]), //pid
//                            split[2], //bid_array
//                            Integer.parseInt(split[3]),//num_of_partition
//                            split[5],//getAccountId
//                            split[6],//getBookEntryId
//                            Integer.parseInt(split[7]),  //getAccountTransfer
//                            Integer.parseInt(split[8])  //getBookEntryTransfer
//                    );
//                } else if (split[4].endsWith("TransactionEvent")) {//TransactionEvent
//                    event = new TransactionEvent(
//                            Integer.parseInt(split[0]), //bid
//                            Integer.parseInt(split[1]), //pid
//                            split[2], //bid_array
//                            Integer.parseInt(split[3]),//num_of_partition
//                            split[5],//getSourceAccountId
//                            split[6],//getSourceBookEntryId
//                            split[7],//getTargetAccountId
//                            split[8],//getTargetBookEntryId
//                            Integer.parseInt(split[9]),  //getAccountTransfer
//                            Integer.parseInt(split[10])  //getBookEntryTransfer
//                    );
//                }
////                db.eventManager.put(input_event, Integer.parseInt(split[0]));
//                myevents[i++] = event;
//                if (i == num_events_per_thread) break;
//                for (int j = 0; j < (tthread - 1) * combo_bid_size; j++) {
//                    if (sc.hasNextLine())
//                        sc.nextLine();//skip un-related.
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        if (enable_debug)
//            show_stats();
//    }

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