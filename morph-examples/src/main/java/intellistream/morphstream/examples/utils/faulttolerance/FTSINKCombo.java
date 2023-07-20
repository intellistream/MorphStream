package intellistream.morphstream.examples.utils.faulttolerance;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;

public class FTSINKCombo extends SINKCombo {
    @Override
    public void execute(Tuple input) throws InterruptedException {
        if (input.getBID() >= lastTask) {
            if (isRecovery && !stopRecovery) {
                MeasureTools.END_RECOVERY_TIME_MEASURE(thisTaskId);
                MeasureTools.END_REPLAY_MEASURE(thisTaskId);
                stopRecovery = true;
            }
            Metrics.RuntimePerformance.lastTasks[thisTaskId] = input.getBID();
            latency_measure(input);
        }
    }
}
