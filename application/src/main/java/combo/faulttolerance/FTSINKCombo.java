package combo.faulttolerance;

import combo.SINKCombo;
import engine.stream.execution.runtime.tuple.impl.Tuple;
import engine.txn.profiler.MeasureTools;
import engine.txn.profiler.Metrics;

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
