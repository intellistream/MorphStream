package metrics;

import components.operators.api.TransactionalBolt;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

class RuntimeMeasureThread implements Runnable {
    private String operatorID;
    private int threadID;
    private boolean signaled;
    TransactionalBolt bolt;

    public RuntimeMeasureThread(String operatorID, int threadID, TransactionalBolt bolt) {
        this.operatorID = operatorID;
        this.threadID = threadID;
        this.bolt = bolt;
        this.signaled = false;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Wait for a signal from the manager
                synchronized (this) {
                    while (!signaled) {
                        wait();
                    }
                    signaled = false;
                }

                // TODO: Fetch runtime performance info from the corresponding bolt thread
                DescriptiveStatistics latencyStats = bolt.getLatencyStats();
                DescriptiveStatistics throughputStats = bolt.getThroughputStats();

                // Send the info back to the manager
                RuntimeManager.updateLatencyStats(operatorID, threadID, latencyStats);
                RuntimeManager.updateThroughputStats(operatorID, threadID, throughputStats);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Method to signal this thread
    public void signal() {
        synchronized (this) {
            signaled = true;
            notify();
        }
    }
}


