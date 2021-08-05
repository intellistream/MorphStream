package transaction;

import transaction.dedicated.ordered.MyList;
import transaction.scheduler.layered.struct.Operation;
import transaction.scheduler.layered.struct.OperationChain;
import content.T_StreamContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import storage.datatype.ListDoubleDataBox;
import transaction.function.*;
import transaction.scheduler.IScheduler;
import transaction.scheduler.SchedulerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static common.CONTROL.*;
import static common.meta.MetaTypes.AccessType.*;

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
    private final HashMap<Integer, Instance> multi_engine = new HashMap<>();//one island one engine.
    private Integer num_op = -1;
    private Integer first_exe;
    private Integer last_exe;
    private CyclicBarrier barrier;
    private Instance standalone_engine;
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support.
    private int app;
    private int TOTAL_CORES;
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
        this.app = app;
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

    public void engine_init(Integer first_exe, Integer last_exe, Integer stage_size, int tp, String schedulerType) {

        scheduler = new SchedulerFactory(tp).CreateScheduler(SchedulerFactory.SCHEDULER_TYPE.valueOf(schedulerType));

        this.first_exe = first_exe;
        this.last_exe = last_exe;
        num_op = stage_size;
        barrier = new CyclicBarrier(stage_size);
        if (enable_work_partition) {
            if (island == -1) {//one engine one core.
                for (int i = 0; i < tp; i++)
                    multi_engine.put(i, new Instance(1));
            } else if (island == -2) {//one engine one socket.
                int actual_island = tp / CORE_PER_SOCKET;
                int i;
                for (i = 0; i < actual_island; i++) {
                    multi_engine.put(i, new Instance(CORE_PER_SOCKET));
                }
                if (tp % CORE_PER_SOCKET != 0) {
                    multi_engine.put(i, new Instance(tp % CORE_PER_SOCKET));
                }
            } else
                throw new UnsupportedOperationException();
        } else {
            standalone_engine = new Instance(tp);
        }
        TOTAL_CORES = tp;
        LOG.info("Engine initialize:" + " Working Threads:" + tp);
    }

    public void engine_shutdown() {
        LOG.info("Shutdown Engine!");
        if (enable_work_partition) {
            for (Instance engine : multi_engine.values()) {
                engine.close();
            }
        } else {
            //single box engine.
            standalone_engine.close();
        }
    }

    // DD: Transfer event processing
    private void CT_Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        // read
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        SchemaRecord preValues1 = operation.condition_records[1].content_.readPreValues(operation.bid);
        if (preValues == null) {
            LOG.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
            LOG.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
                LOG.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            LOG.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
        }
        if (preValues1 == null) {
            LOG.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
            LOG.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
                LOG.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            LOG.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
        }
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
        final long sourceAssetValue = preValues1.getValues().get(1).getLong();
        //when d_record is different from condition record
        //It may generate cross-records dependency problem.
        //Fix it later.
        // check the preconditions
        //TODO: make the condition checking more generic in future.

        // DD: Transaction Operation is conditioned on both source asset and account balance. So the operation can depend on both.
        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2
                && sourceAssetValue > operation.condition.arg3) {
            //read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            List<DataBox> values = srcRecord.getValues();
            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record
            //apply function.
            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            //Operation.d_record.content_.WriteAccess(Operation.bid, new SchemaRecord(values), wid);//does this even needed?
            operation.success[0] = true;
        } else {
            operation.success[0] = false;
        }
    }

    private void CT_Depo_Fun(Operation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(operation.column_id).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    //TODO: the following are mostly hard-coded.
    private void process(Operation operation, long mark_ID, boolean clean) {
        if (operation.accessType == READS_ONLY) {
            operation.records_ref.setRecord(operation.d_record);
        } else if (operation.accessType == READ_ONLY) {//used in MB.
//            operation.record_ref.inc(Thread.currentThread().getName());
            //read source.
//            List<DataBox> dstRecord = operation.d_record.content_.ReadAccess(operation.bid, operation.accessType).getValues();
            SchemaRecord schemaRecord = operation.d_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType);
            operation.record_ref.setRecord(new SchemaRecord(schemaRecord.getValues()));//Note that, locking scheme allows directly modifying on original table d_record.
//            if (operation.record_ref.cnt == 0) {
//                System.out.println("Not assigning");
//                System.exit(-1);
//            }
        } else if (operation.accessType == WRITE_ONLY) {//push evaluation down. //used in MB.
            if (operation.value_list != null) { //directly replace value_list --only used for MB.
                //read source.
//                List<DataBox> dstRecord = operation.d_record.content_.ReadAccess(operation.bid, operation.accessType).getValues();
//                if (enable_mvcc) {
//
//                }else {
//                operation.d_record.record_.updateValues(operation.value_list);
//                }
//                operation.d_record.record_.s.get(1).setString(values.get(1).getString(), VALUE_LEN);
                operation.d_record.content_.WriteAccess(operation.bid, mark_ID, clean, new SchemaRecord(operation.value_list));//it may reduce NUMA-traffic.
            } else { //update by column_id.
                operation.d_record.record_.getValues().get(operation.column_id).setLong(operation.value);
//                LOG.info("Alert price:" + operation.value);
            }
        } else if (operation.accessType == READ_WRITE) {//read, modify, write.
            if (app == 1) {
                CT_Depo_Fun(operation, mark_ID, clean);//used in SL
            } else {
                SchemaRecord srcRecord = operation.s_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType);
                List<DataBox> values = srcRecord.getValues();
                //apply function to modify..
                if (operation.function instanceof INC) {
                    values.get(operation.column_id).setLong(values.get(operation.column_id).getLong() + operation.function.delta_long);
                } else
                    throw new UnsupportedOperationException();
            }
        } else if (operation.accessType == READ_WRITE_COND) {//read, modify (depends on condition), write( depends on condition).
            //TODO: pass function here in future instead of hard-code it. Seems not trivial in Java, consider callable interface?
            if (app == 1) {//used in SL
                CT_Transfer_Fun(operation, mark_ID, clean);
            } else if (app == 2) {//used in OB
                //check if any item is not able to buy.
                List<DataBox> d_record = operation.condition_records[0].content_
                        .ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = operation.condition.arg1;
                long bid_qty = operation.condition.arg2;
                // check the preconditions
                if (bidPrice < askPrice || bid_qty > left_qty) {
                    operation.success[0] = false;
                } else {
                    d_record.get(2).setLong(left_qty - operation.function.delta_long);//new quantity.
                    operation.success[0] = true;
                }
            }
        } else if (operation.accessType == READ_WRITE_COND_READ) {
            assert operation.record_ref != null;
            if (app == 1) {//used in SL
                CT_Transfer_Fun(operation, mark_ID, clean);
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            } else
                throw new UnsupportedOperationException();
//            if (operation.record_ref.cnt == 0) {
//                System.out.println("Not assigning");
//                System.exit(-1);
//            }
        } else if (operation.accessType == READ_WRITE_READ) {//used in PK, TP.
            assert operation.record_ref != null;
            //read source.
            List<DataBox> srcRecord = operation.s_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
            //apply function.
            if (operation.function instanceof Mean) {
                // compute.
                ListDoubleDataBox valueList = (ListDoubleDataBox) srcRecord.get(1);
                double sum = srcRecord.get(2).getDouble();
                double[] nextDouble = operation.function.new_value;
                for (int j = 0; j < 50; j++) {
                    sum -= valueList.addItem(nextDouble[j]);
                    sum += nextDouble[j];
                }
                // update content.
                srcRecord.get(2).setDouble(sum);
                // Operation.d_record.content_.WriteAccess(Operation.bid, new SchemaRecord(srcRecord), wid);//not even needed.
                // configure return-record.
                if (valueList.size() < 1_000) {//just added
                    operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(nextDouble[50 - 1])));
                } else {
                    operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(sum / 1_000)));
                }
//                if (operation.record_ref.cnt == 0) {
//                    System.out.println("Not assigning");
//                    System.exit(-1);
//                }
//                            LOG.info("BID:" + Operation.bid + " is set @" + DateTime.now());
            } else if (operation.function instanceof AVG) {//used by TP
                //                double lav = (latestAvgSpeeds + speed) / 2;//compute the average.
                double latestAvgSpeeds = srcRecord.get(1).getDouble();
                double lav;
                if (latestAvgSpeeds == 0) {//not initialized
                    lav = operation.function.delta_double;
                } else
                    lav = (latestAvgSpeeds + operation.function.delta_double) / 2;
                srcRecord.get(1).setDouble(lav);//write to state.
                operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
            } else if (operation.function instanceof CNT) {//used by TP
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
                operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
            } else
                throw new UnsupportedOperationException();
        }
    }

    //TODO: actual evaluation on the operation_chain.
    private void process(MyList<Operation> operation_chain, long mark_ID) {

        Operation operation = operation_chain.pollFirst();//multiple threads may work on the same operation chain, use MVCC to preserve the correctness. // Right now: 1 thread executes 1 OC.
        while (operation != null) {
            process(operation, mark_ID, false);
            operation = operation_chain.pollFirst();
        }//loop.

    }

    public IScheduler getScheduler() {
        return this.scheduler;
    }

    public void start_evaluation(int threadId, long mark_ID) throws InterruptedException {
        OperationChain oc = scheduler.NEXT(threadId);
        while (oc != null) {
            MyList<Operation> operations = oc.getOperations();
            process(operations, mark_ID);//directly apply the computation.
            oc = scheduler.NEXT(threadId);

        }
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