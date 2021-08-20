package execution.runtime;

import common.collections.Configuration;
import components.TopologyComponent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static common.CONTROL.enable_log;

//import static xerial.jnuma.Numa.newCPUBitMask;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public abstract class executorThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(executorThread.class);
    public final ExecutionNode executor;
    protected final CountDownLatch latch;
    final Configuration conf;
    private final HashMap<Integer, executorThread> threadMap;
    public boolean running = true;
    public boolean profiling = false;
    public long[] cpu;
    public int node;
    public boolean migrating = false;
    double expected_throughput = 0;
    boolean not_yet_profiled = true;
    TopologyContext context;//every thread owns its unique context, which will be pushed to its emitting tuple.
    double cnt = 0;
    long start_emit = 0;
    long end_emit = 0;
    int batch;
    private boolean start = true;
    private volatile boolean ready = false;

    protected executorThread(ExecutionNode e, Configuration conf, TopologyContext context
            , long[] cpu, int node, CountDownLatch latch, HashMap<Integer, executorThread> threadMap) {
        this.context = context;
        this.conf = conf;
        executor = e;
        this.cpu = cpu;
        this.node = node;
        this.latch = latch;

        this.threadMap = threadMap;
        if (executor != null && !this.executor.isLeafNode()) {
            this.executor.getController().setContext(this.executor.getExecutorID(), context);
        }
    }

    public TopologyContext getContext() {
        return context;
    }

    public void setContext(TopologyContext context) {
        this.context = context;
    }

    //    private long[] convertToCPUMasK(long[] cpu) {
//        final long[] cpuMask = newCPUBitMask();
//        if(enable_log) LOG.info("Empty cpuMask:" + Arrays.toString(cpuMask));
//        for (long i : cpu) {
//            cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a single CPU on
//        }
//        if(enable_log) LOG.info("Configured cpuMask:" + Arrays.toString(cpuMask));
//        return cpuMask;
//    }
    public void initilize_queue(int executorID) {
        allocate_OutputQueue();
        assign_InputQueue();
    }

    private void pause() {
        for (TopologyComponent children : this.executor.getChildren_keySet()) {
            for (ExecutionNode c : children.getExecutorList()) {
                if (threadMap.get(c.getExecutorID()) != null) {
                    threadMap.get(c.getExecutorID()).suspend();
                } else {
                    if(enable_log) LOG.info(c.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void pause_parent() {
        for (TopologyComponent parent : this.executor.getParents_keySet()) {
            for (ExecutionNode p : parent.getExecutorList()) {
                if (threadMap.get(p.getExecutorID()) != null) {
                    threadMap.get(p.getExecutorID()).suspend();
                } else {
                    if(enable_log) LOG.info(p.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void restart() {
        for (TopologyComponent children : this.executor.getChildren_keySet()) {
            for (ExecutionNode c : children.getExecutorList()) {
                if (threadMap.get(c.getExecutorID()) != null) {
                    threadMap.get(c.getExecutorID()).resume();
                } else {
                    if(enable_log) LOG.info(c.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void restart_parents() {
        for (TopologyComponent parent : this.executor.getParents_keySet()) {
            for (ExecutionNode p : parent.getExecutorList()) {
                if (threadMap.get(p.getExecutorID()) != null) {
                    threadMap.get(p.getExecutorID()).resume();
                } else {
                    if(enable_log) LOG.info(p.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void allocate_OutputQueue() {
        executor.allocate_OutputQueue(conf.getBoolean("linked", false), conf.getInt("queue_size"));
    }

    private void assign_InputQueue(String streamId) {
        executor.setReceive_queueOfChildren(streamId);
    }

    /**
     * Assign my output queue to my downstream executor.
     */
    private void assign_InputQueue() {
        for (String streamId : executor.operator.getOutput_streamsIds()) {
            assign_InputQueue(streamId);
        }
    }

    HashMap<Integer, Queue> get_receiving_queue(String streamId) {
        return executor.getInputStreamController().getReceive_queue(streamId);
    }

    HashMap<String, HashMap<Integer, Queue>> get_receiving_queue() {
        return executor.getInputStreamController().getRQ();
    }

    void routing() throws InterruptedException, DatabaseException, BrokenBarrierException {
//        int s = 0;
        if (start) {
            cnt = 0;
            start_emit = System.nanoTime();
            start = false;
        }
        while (running) {
            _execute();
        }
        end_emit = System.nanoTime();
    }

    protected abstract void _execute_noControl() throws InterruptedException, DatabaseException, BrokenBarrierException;

    protected abstract void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException;

    public int getExecutorID() {
        return executor.getExecutorID();
    }

    public String getOP() {
        return executor.getOP();
    }

    public double getResults() {
        return executor.op.getResults();
    }

    public boolean isReady() {
        return ready;
    }

    void Ready(Logger LOG) {
        //LOG.DEBUG("BasicBoltBatchExecutor:" + executor.getExecutorID() + " is set to ready");
        ready = true;
    }
}
