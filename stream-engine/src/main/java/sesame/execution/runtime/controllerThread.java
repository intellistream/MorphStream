package sesame.execution.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This can be used for monitor thread performance online.
 */
class controllerThread extends Thread {
    private final static Logger LOG = LoggerFactory.getLogger(controllerThread.class);
    private final executorThread hostThread;
    private double start;
    private long StartTime;

    private controllerThread(executorThread hostThread) {
        this.hostThread = hostThread;
        double expectedRate = hostThread.expected_throughput;
    }

    private void measure_start() {
        start = hostThread.cnt;
        StartTime = System.currentTimeMillis();
    }

    private void measure_end() {
        long endTime = System.currentTimeMillis();
        double end = hostThread.cnt;
        double observedRate = (end - start) / (endTime - StartTime);
//        if (ExpectedRate > ObservedRate) {
//            hostThread.incParallelism();
//        }
        LOG.info("current throughput of " + hostThread.executor.getOP() + observedRate);
    }

    @Override
    public void run() {
        while (true) {
            measure_start();
            try {
                sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            measure_end();
        }
    }
}
