package sesame.execution.runtime;

import application.util.Configuration;
import ch.usi.overseer.OverHpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.components.operators.executor.BoltExecutor;
import sesame.controller.input.InputStreamController;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.optimization.OptimizationManager;
import sesame.optimization.model.STAT;
import state_engine.Clock;
import state_engine.DatabaseException;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static application.CONTROL.enable_shared_state;

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

    public volatile boolean binding_finished = false;
    private boolean UNIX = false;
    private int miss = 0;

    /**
     * @param e
     * @param context
     * @param conf
     * @param cpu
     * @param node
     * @param latch
     * @param HPCMonotor
     * @param optimizationManager
     * @param threadMap
     * @param clock
     */
    public boltThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu
            , int node, CountDownLatch latch, OverHpc HPCMonotor, OptimizationManager optimizationManager
            , HashMap<Integer, executorThread> threadMap, Clock clock) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        bolt = (BoltExecutor) e.op;
        scheduler = e.getInputStreamController();
        this.collector = new OutputCollector(e, context);
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

//        Tuple in = fetchResult_single();
//        if (in != null) {
//            bolt.execute(in);
//            cnt += batch;
//        } else {
//            miss++;
//        }

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


//        if (enable_shared_state) {//this is for T-Stream.
//            Tuple in = fetchResult_single();
//            if (in != null) {
//                bolt.execute(in);
//                cnt += batch;
//            } else {
//                miss++;
//            }
//        } else {
//            JumboTuple in = fetchResult();
//            if (in != null) {
//                bolt.execute(in);
//                cnt += batch;
//            } else {
//                miss++;
//            }
//        }

    }

    protected void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    public void run() {

        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());
            if (TopologyContext.plan.getSP() != null) {
                for (String Istream : new HashSet<>(executor.operator.input_streams)) {
//					for (String Ostream : new HashSet<>(executor.operator.getOutput_streamsIds())) {
                    expected_throughput += executor.getExpectedProcessRate(Istream, TopologyContext.plan.getSP(), false) * 1E6;
//					}
                }
            }

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
     * @since 0.0.7 we add a tuple txn module so that we can support customized txn rules in Brisk.execution.runtime.tuple fetching.
     */
    private Object fetchResult() {

        return scheduler.fetchResults();
//		return scheduler.fetchResults_inorder();


    }

    private Tuple fetchResult_single() {

        return scheduler.fetchResults_single();
//		return scheduler.fetchResults_inorder();


    }


    private JumboTuple fetchResult(STAT stat, int batch) {
        return scheduler.fetchResults(stat, batch);
    }


    private JumboTuple fetchResult(ExecutionNode src, STAT stat, int batch) {
        return scheduler.fetchResults(src, stat, batch);
    }

}
