package execution.runtime;

import common.collections.Configuration;
import components.context.TopologyContext;
import components.operators.executor.BasicSpoutBatchExecutor;
import db.DatabaseException;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static common.CONTROL.*;
import static content.Content.*;

/**
 * Task thread that hosts spout logic.
 */
public class spoutThread extends executorThread {
    private static final Logger LOG = LoggerFactory.getLogger(spoutThread.class);
    private final BasicSpoutBatchExecutor sp;
    private final OutputCollector collector;
    int _combo_bid_size = 1;

    /**
     * @param e         :                  Each thread corresponds to one executionNode.
     * @param conf
     * @param cpu
     * @param node
     * @param latch
     * @param threadMap
     */
    public spoutThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu,
                       int node, CountDownLatch latch,
                       HashMap<Integer, executorThread> threadMap) {
        super(e, conf, context, cpu, node, latch, threadMap);
        this.sp = (BasicSpoutBatchExecutor) e.op;
        this.collector = new OutputCollector(e, context, conf.getInt("totalEvents"));
        batch = conf.getInt("batch", 100);
        sp.setExecutionNode(e);
        switch (conf.getInt("CCOption", 0)) {
            case CCOption_OrderLOCK://Ordered lock_ratio
            case CCOption_LWM://LWM
            case CCOption_SStore://SStore
                _combo_bid_size = 1;
                break;
            default:
                _combo_bid_size = combo_bid_size;
        }
    }

    @Override
    protected void _execute_noControl() throws InterruptedException {
        sp.bulk_emit(batch);
        if (enable_app_combo)
            cnt += batch * _combo_bid_size;
        else
            cnt += batch;
    }

    protected void _execute() throws InterruptedException {

        _execute_noControl();

    }

    protected void _execute_with_index(int index) throws InterruptedException {
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());
            binding();
            initilize_queue(this.executor.getExecutorID());
            //do Loading
            sp.prepare(conf, context, collector);
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            if (enable_log) LOG.info("Operator:\t" + executor.getOP_full() + " is ready");
            this.Ready(LOG);//Tell executor thread to proceed.
            latch.countDown();          //tells others I'm really ready.
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
            routing();
        } catch (InterruptedException | DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        } finally {
            this.executor.display();
            if (end_emit == 0) {
                end_emit = System.nanoTime();
            }
            double actual_throughput = (cnt - this.executor.op.getEmpty()) * 1E6 / (end_emit - start_emit);

            if (enable_log) LOG.info(this.executor.getOP_full()
                    + "\tfinished execution and exit with throughput (k input_event/s) of:\t"
                    + actual_throughput
                    + " on node: " + node
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                //e.printStackTrace();
            }
        }

    }

}
