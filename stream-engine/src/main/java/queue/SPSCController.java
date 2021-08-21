package queue;

import common.collections.OsUtils;
import execution.ExecutionNode;
import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Queue;

import static common.CONTROL.enable_log;

/**
 * Created by shuhaozhang on 11/7/16.
 * There's one PC per pair of "downstream, downstream operator".
 * PC is owned by streamController, which is owned by each executor.
 */
public class SPSCController extends QueueController {
    private static final Logger LOG = LoggerFactory.getLogger(SPSCController.class);
    private static final long serialVersionUID = -5079796097938215439L;
    private final HashMap<Integer, Queue> outputQueue = new HashMap<>();//<Downstream executor ID, corresponding output queue>

    /**
     * This is where partition ratio is being updated.
     *
     * @param downExecutor_list
     */
    public SPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
    }

    public boolean isEmpty() {
        for (int executor : downExecutor_list.keySet()) {
            Queue queue = outputQueue.get(executor);
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Allocate memory for queue structure here.
     *
     * @param linked
     * @param desired_elements_epoch_per_core
     */
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {
//		serializer = new Serializer();
        for (int executor : downExecutor_list.keySet()) {
            //clean_executorInformation the queue if it exist
            Queue queue = outputQueue.get(executor);
            if (queue != null) {
//                if(queue instanceof P1C1Queue)
                if (enable_log) LOG.info("relax_reset the old queue");
                queue.clear();
                System.gc();
            }
            if (OsUtils.isWindows() || OsUtils.isMac()) {
                outputQueue.put(executor, new SpscArrayQueue(1024));
            } else {
                if (linked) {
                    outputQueue.put(executor, new SpscLinkedQueue()/*new P1C1Queue<JumboTuple>()*/);
                } else {
                    outputQueue.put(executor, new SpscArrayQueue(desired_elements_epoch_per_core / 2)/*new P1C1Queue<JumboTuple>()*/);
                }
            }
        }
    }

    public Queue get_queue(int executor) {
        return outputQueue.get(executor);
    }
}

