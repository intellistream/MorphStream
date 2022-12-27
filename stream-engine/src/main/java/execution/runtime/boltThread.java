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
import optimization.OptimizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_shared_state;
import static common.CONTROL.fetchWithIndex;

/**
 * Task thread that hosts bolt logic. Receives input Brisk.execution.runtime.tuple,
 * processes it using bolt logic, forwards to next bolts in
 * Brisk.topology.
 */
public class boltThread extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(boltThread.class);
    private final BoltExecutor bolt;
    private final OutputCollector collector;
    private final InputStreamController inputStreamController;
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
     */
    public boltThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu
            , int node, CountDownLatch latch, OptimizationManager optimizationManager
            , HashMap<Integer, executorThread> threadMap) {
        super(e, conf, context, cpu, node, latch, threadMap);
        bolt = (BoltExecutor) e.op;
        inputStreamController = e.getInputStreamController();
        this.collector = new OutputCollector(e, context, conf.getInt("totalEvents"));
        batch = conf.getInt("batch", 100);
        bolt.setExecutionNode(e);
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

    ExecutorService inputExecutor = Executors.newCachedThreadPool();
    Callable<Object> task = new Callable<Object>() {
        public Object call() {
            return fetchResult();
        }
    };
    Future<Object> future = inputExecutor.submit(task);

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
                LOG.info(this.executor.toString() + "get tuple:" + ((Tuple) tuple).getBID());
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

    protected void _execute_noControl_index(int index) throws InterruptedException, DatabaseException, BrokenBarrierException {
        Object tuple = fetchResultIndex(index);
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

    protected void _execute_with_index(int index) throws InterruptedException, DatabaseException, BrokenBarrierException {
        _execute_noControl_index(index);
    }

    private int getBoltIndex(String name, int id) {
        int tthread = conf.getInt("tthread");
        if (!Objects.equals(name, "sink")) { //bolt or gate
            return id % (tthread + 1);
        } else { //sink
            return -1;
        }
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());

            binding();
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

            if (fetchWithIndex) {
                int index = getBoltIndex(executor.getOP(), executor.getExecutorID());
                if (index > 0) { //normal bolt, not gate or sink
                    routing_with_index(index); //bolt id start from 1
                } else { //gate or sink
                    routing();
                }
            } else {
                routing();
            }

            LOG.info("Bolt thread finished routing");

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
            if (enable_log) LOG.info(this.executor.getOP_full()
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
        return inputStreamController.fetchResults();
    }

    private Object fetchResultIndex(int index) {
        return inputStreamController.fetchResultsIndex(index);
    }
}
