package intellistream.morphstream.engine.txn.profiler;

import java.time.Duration;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricRegistryImpl {
    private final ScheduledExecutorService reporterScheduledExecutor;
    public MetricRegistryImpl(ScheduledExecutorService scheduledExecutor) {
        this.reporterScheduledExecutor = scheduledExecutor;
        final Reporter reporterInstance = new Reporter();
        reporterInstance.open("localhost", 4200);
//        profiler.Reporter.Initialize(4);
        Duration duration = Duration.ofSeconds(1); // set duration to 10 seconds
        this.reporterScheduledExecutor.scheduleWithFixedDelay(
                new MetricRegistryImpl.ReporterTask(reporterInstance),
                duration.toMillis(),
                duration.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private static final class ReporterTask extends TimerTask {
        private final Scheduled reporter;

        private ReporterTask(Scheduled reporter) {
            this.reporter = reporter;
        }

        /**
         * Main task for reporter task
         */
        @Override
        public void run() {
            try {
                reporter.report();
                System.out.println("HAHA");
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}
