package intellistream.morphstream.engine.txn.scheduler.impl.op.structured;

import intellistream.morphstream.engine.txn.scheduler.context.op.OPSContext;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OPBFSScheduler<Context extends OPSContext> extends OPSScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPBFSScheduler.class);


    public OPBFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(Context context) {
        Operation next = Next(context);
        while (next == null && !context.finished()) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            ProcessedToNextLevel(context);
            next = Next(context);
        }
        DISTRIBUTE(next, context);
    }
}