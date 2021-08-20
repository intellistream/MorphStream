package transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.SchedulerFactory;
import scheduler.context.SchedulerContext;
import scheduler.impl.IScheduler;

import static common.CONTROL.enable_log;

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

    public void engine_init(int tthread, int NUM_ITEMS, String schedulerType) {
        scheduler = new SchedulerFactory(tthread, NUM_ITEMS).CreateScheduler(SchedulerFactory.SCHEDULER_TYPE.valueOf(schedulerType));
        if(enable_log) LOG.info("Engine initialize:" + " Total Working Threads:" + tthread);
    }

    public void engine_shutdown() {
        if(enable_log) LOG.info("Shutdown Engine!");
    }

    public IScheduler getScheduler() {
        return this.scheduler;
    }

    public void start_evaluation(SchedulerContext context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;
//        MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        scheduler.INITIALIZE(context);
//        MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);

        do {
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            scheduler.EXPLORE(context);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            scheduler.PROCESS(context, mark_ID);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
        } while (!scheduler.FINISHED(context));
        scheduler.RESET();//
        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
    }
}