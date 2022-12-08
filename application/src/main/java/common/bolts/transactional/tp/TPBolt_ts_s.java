package common.bolts.transactional.tp;

import combo.SINKCombo;
import common.param.lr.LREvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.AVG;
import transaction.function.CNT;
import transaction.function.Condition;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.enable_latency_measurement;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static profiler.Metrics.NUM_ITEMS;

public class TPBolt_ts_s extends TPBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TPBolt_ts_s.class);
    ArrayDeque<LREvent> LREvents;
    public TPBolt_ts_s(int fid,SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TPBolt_ts_s(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager=new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(),thread_Id,
                NUM_ITEMS,this.context.getThisComponent().getNumTasks(),config.getString("scheduler","BF"));
        LREvents = new ArrayDeque<>();
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()){
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            int readSize=LREvents.size();
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            {
                transactionManager.start_evaluate(thread_Id,in.getBID(),readSize);
                REQUEST_REQUEST_CORE();
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
            MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
            {
                REQUEST_POST();
            }
            MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
            //all tuples in the holder is finished.
            LREvents.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id,readSize);
        }else{
            execute_ts_normal(in);
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(double bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for(double i=_bid;i<_bid+_combo_bid_size;i++){
            TxnContext txnContext=new TxnContext(thread_Id,this.fid,i);
            LREvent event=(LREvent) input_event;
            if(enable_latency_measurement){
                (event).setTimestamp(timestamp);
            }
            REQUEST_CONSTRUCT(event,txnContext);
        }
    }
    protected void REQUEST_CONSTRUCT(LREvent event,TxnContext txnContext) throws DatabaseException{
        transactionManager.BeginTransaction(txnContext);
//        transactionManager.Asy_ModifyRecord_Read(txnContext
//                , "segment_speed"
//                , String.valueOf(event.getPOSReport().getSegment())
//                , event.speed_value//holder to be filled up.
//                , new AVG(event.getPOSReport().getSpeed())
//        );          //asynchronously return.
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed", String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value
                , new AVG(event.getPOSReport().getSpeed())
                , new Condition(event.getPOSReport().getSpeed(),200)
                , event.success
                , "tp");
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , new CNT(event.getPOSReport().getVid())
                , "tp"
        );          //asynchronously return.
        transactionManager.CommitTransaction(txnContext);
        LREvents.add(event);
    }
    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(LREvent event) {
        if (event.success[0] != 0){
            event.count = event.count_value.getRecord().getValue().getInt();
            event.lav = event.speed_value.getRecord().getValue().getDouble();
        }
    }
    protected void REQUEST_POST() throws InterruptedException {
        for (LREvent event : LREvents) {
            REQUEST_POST(event);
        }
    }
}
