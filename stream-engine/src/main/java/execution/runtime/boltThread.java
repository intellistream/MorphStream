package execution.runtime;

import common.collections.Configuration;
import components.context.TopologyContext;
import components.operators.executor.BoltExecutor;
import controller.input.InputStreamController;
import db.DatabaseException;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Tuple;
import lock.Clock;
import optimization.OptimizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static common.CONTROL.enable_shared_state;

/**
 * Task thread that hosts bolt logic. Receives input Brisk.execution.runtime.tuple,
 * processes it using bolt logic, forwards to next bolts in
 * Brisk.topology.
 */
public class boltThread extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(boltThread.class);
    private final BoltExecutor bolt;
    private final OutputCollector collector;
    private final InputStreamController scheduler;
    private int miss = 0;

    /**
     * @param e
     * @param context
     * @param conf
     * @param cpu
     * @param node
     * @param latch
     * @param optimizationManager
     * @param threadMap
     * @param clock
     */
    public boltThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu
            , int node, CountDownLatch latch, OptimizationManager optimizationManager
            , HashMap<Integer, executorThread> threadMap, Clock clock) {
        super(e, conf, context, cpu, node, latch, threadMap);
        bolt = (BoltExecutor) e.op;
        scheduler = e.getInputStreamController();
        this.collector = new OutputCollector(e, context, conf.getInt("totalEventsPerBatch") * conf.getInt("numberOfBatches"));
        batch = conf.getInt("batch", 100);
        bolt.setExecutionNode(e);
        bolt.setclock(clock);
    }

    /**
     * @author Crunchify.com
     */
    public static String crunchifyGenerateThreadDump() {
        final StringBuilder dump = new StringBuilder();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    /**
     * Be very careful for this method..
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    protected void _execute_noControl() throws InterruptedException, DatabaseException, BrokenBarrierException {
        Object tuple = fetchResult();
        if (tuple instanceof Tuple) {
            if (tuple != null) {
                bolt.execute((Tuple) tuple);
                cnt += 1;
            } else {
                miss++;
            }
        } else {
            if (tuple != null) {
                bolt.execute((JumboTuple) tuple);
                cnt += batch;
            } else {
                miss++;
            }
        }
    }

    protected void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());

            initilize_queue(this.executor.getExecutorID());
            //do preparation.
            bolt.prepare(conf, context, collector);
            this.Ready(LOG);
            if (enable_shared_state)
                if (!this.executor.isLeafNode())//TODO: remove such hard code in future.
                    bolt.loadDB(conf, context, collector);
            latch.countDown();          //tells others I'm ready.
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            routing();
        } catch (InterruptedException | BrokenBarrierException ignored) {
        } catch (DatabaseException e) {
            e.printStackTrace();
        } finally {
            this.executor.display();
            if (end_emit == 0) {
                end_emit = System.nanoTime();
            }
            double actual_throughput = cnt * 1E6 / (end_emit - start_emit);
            if (expected_throughput == 0) {
                expected_throughput = actual_throughput;
            }
            LOG.info(this.executor.getOP_full()
                            + "\tfinished execution and exist with throughput of:\t"
                            + actual_throughput + "(" + (actual_throughput / expected_throughput) + ")"
                            + " on node: " + node + " fetch miss rate:" + miss / (cnt + miss) * 100
//					+ " ( " + Arrays.show(cpu) +")"
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Get input from upstream bolts, this is a unique function of bolt thread.
     * TODO: need a txn module to determine the fetch sequence.
     *
     * @since 0.0.7 we addOperation a tuple txn module so that we can support customized txn rules in Brisk.execution.runtime.tuple fetching.
     */
    private Object fetchResult() {
        return scheduler.fetchResults();
    }
}
