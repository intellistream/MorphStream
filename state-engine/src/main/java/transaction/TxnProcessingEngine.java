package transaction;

import content.T_StreamContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import storage.datatype.ListDoubleDataBox;
import transaction.dedicated.ordered.MyList;
import transaction.function.*;
import transaction.scheduler.IScheduler;
import transaction.scheduler.SchedulerFactory;
import transaction.scheduler.layered.struct.Operation;
import transaction.scheduler.layered.struct.OperationChain;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static common.CONTROL.*;
import static common.meta.CommonMetaTypes.AccessType.*;

/**
 * There is one TxnProcessingEngine of each stage.
 * This is closely bundled with the start_ready map.
 * <p>
 * It now only works for single stage..
 */
public final class TxnProcessingEngine {
    private static final Logger LOG = LoggerFactory.getLogger(TxnProcessingEngine.class);
    private static final TxnProcessingEngine instance = new TxnProcessingEngine();
    /**
     * @param threadId
     * @param mark_ID
     * @return time spend in tp evaluation.
     * @throws InterruptedException
     */
    //    fast determine the corresponding instance. This design is for NUMA-awareness.
    //one island one engine.
    private Integer num_op = -1;
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support.

    //TODO: select which scheduler to initialize.
    private IScheduler scheduler;

    private TxnProcessingEngine() {
    }

    /**
     * @return
     */
    public static TxnProcessingEngine getInstance() {
        return instance;
    }

    public void initilize(int size, int app) {
        num_op = size;
        //        holder_by_stage = new Holder_in_range(num_op);
        holder_by_stage = new ConcurrentHashMap<>();
        //make it flexible later.
        if (app == 1)//SL
        {
            holder_by_stage.put("accounts", new Holder_in_range(num_op));
            holder_by_stage.put("bookEntries", new Holder_in_range(num_op));
        } else if (app == 2) {//OB
            holder_by_stage.put("goods", new Holder_in_range(num_op));
        } else if (app == 3) {//TP
            holder_by_stage.put("segment_speed", new Holder_in_range(num_op));
            holder_by_stage.put("segment_cnt", new Holder_in_range(num_op));
        } else {//MB or S_STORE
            holder_by_stage.put("MicroTable", new Holder_in_range(num_op));
        }
    }

    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }

    public ConcurrentHashMap<String, Holder_in_range> getHolder() {
        return holder_by_stage;
    }

    public void engine_init(Integer stage_size, int tthread, String schedulerType) {
        scheduler = new SchedulerFactory(tthread).CreateScheduler(SchedulerFactory.SCHEDULER_TYPE.valueOf(schedulerType));
        num_op = stage_size;
        LOG.info("Engine initialize:" + " Total Working Threads:" + tthread);
    }

    public void engine_shutdown() {
        LOG.info("Shutdown Engine!");
    }

    public IScheduler getScheduler() {
        return this.scheduler;
    }

    public void start_evaluation(int threadId, long mark_ID, int num_events) throws InterruptedException {
        MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        scheduler.INITIALIZE(threadId);
        MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);

        do {
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            scheduler.EXPLORE(threadId);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            scheduler.PROCESS(threadId, mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (!scheduler.FINISHED(threadId));

        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
        scheduler.RESET();//
    }

    /**
     * There shall be $num_op$ Holders.
     */
    public class Holder {
        //version 1: single list Operation on one key
        //	ConcurrentSkipListSet holder_v1 = new ConcurrentSkipListSet();
        public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>(100000);
//        public ConcurrentHashMap<String, MyList<Operation>> holder_v1 = new ConcurrentHashMap<>();
//        public ConcurrentSkipListSet<Operation>[] holder_v2 = new ConcurrentSkipListSet[H2_SIZE];
    }

    //     DD: We basically keep multiple holders and distribute operations among them.
//     For example, holder with key 1 can hold operations on tuples with key, 1-100,
//     holder with key 2 can hold operations on tuples with key 101-200 and so on...
    public class Holder_in_range {
        public ConcurrentHashMap<Integer, Holder> rangeMap = new ConcurrentHashMap<>();//each op has a holder.

        public Holder_in_range(Integer num_op) {
            int i;
            for (i = 0; i < num_op; i++) {
                rangeMap.put(i, new Holder());
            }
        }
    }

    /**
     * TP-processing instance.
     * If it is one, it is a shared everything configuration.
     * Otherwise, we use hash partition to distribute the data and workload.
     */
    class Instance implements Closeable {
        public ExecutorService executor;
        int range_min;
        int range_max;

        //        ExecutorCompletionService<Integer> TP_THREADS; // In the next work, we can try asynchronous return from the TP-Layer!
        public Instance(int tpInstance, int range_min, int range_max) {
            this.range_min = range_min;
            this.range_max = range_max;
            if (enable_work_partition) {
                if (island == -1) {//one core one engine. there's no meaning of stealing.
                    executor = Executors.newSingleThreadExecutor();//one core one engine.
                } else if (island == -2) {//one socket one engine.
                    if (enable_work_stealing) {
                        executor = Executors.newWorkStealingPool(tpInstance);//shared, stealing.
                    } else
                        executor = Executors.newFixedThreadPool(tpInstance);//shared, no stealing.
                } else
                    throw new UnsupportedOperationException();//TODO: support more in future.
            } else {
                if (enable_work_stealing) {
                    executor = Executors.newWorkStealingPool(tpInstance);//shared, stealing.
                } else
                    executor = Executors.newFixedThreadPool(tpInstance);//shared, no stealing.
            }
        }

        /**
         * @param tpInstance
         */
        public Instance(int tpInstance) {
            this(tpInstance, 0, 0);
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }
}