package intellistream.morphstream.engine.txn.profiler;

import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.components.operators.api.delete.TransactionalBolt;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

class RuntimeMeasureThread implements Runnable {
    private String operatorID;
    private int threadID;
    private boolean signaled;
    AbstractBolt bolt;

    public RuntimeMeasureThread(String operatorID, int threadID, AbstractBolt bolt) {
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
                DescriptiveStatistics latencyStats = bolt.getLatencyStats(); //try to get the latest latency from bolt (per batch), if bolt not ready, let measure thread wait
                double throughputStats = bolt.getThroughputStats();

                // Send the info back to the manager
//                Metrics.RuntimeManager.updateLatencyStats(operatorID, threadID, latencyStats);
//                Metrics.RuntimeManager.updateThroughputStats(operatorID, threadID, throughputStats);
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


