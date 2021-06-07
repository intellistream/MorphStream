package controller.output.partition;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import common.collections.Configuration;
import components.TopologyComponent;
import execution.ExecutionNode;
import execution.runtime.collector.impl.Meta;
import execution.runtime.tuple.impl.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by shuhaozhang on 11/7/16.
 * Every stream corresponds a partitionController, who owns the output queue for each downstream executor.
 */
public class PartialKeyGroupingController extends FieldsPartitionController {
    private static final long serialVersionUID = -7938271195232530840L;
    private static final Logger LOG = LoggerFactory.getLogger(PartialKeyGroupingController.class);
    private final HashFunction h1 = Hashing.murmur3_128(13);
    private final HashFunction h2 = Hashing.murmur3_128(17);
    //	private Set<Integer> targetTasks;
    private final long[] targetTaskStats;

    public PartialKeyGroupingController(
            TopologyComponent operator, TopologyComponent childOP, HashMap<Integer,
            ExecutionNode> executionNodeHashMap,
            Fields output_fields, Fields input_fields, int batch_size, ExecutionNode executor, boolean profile, Configuration conf) {
        super(operator, childOP, executionNodeHashMap, output_fields, input_fields, batch_size, executor, profile, conf);
        targetTaskStats = new long[this.targetTasks.length];
    }

    public void initilize() {
//		this.targetTasks = getDownExecutor_list().keySet();
//		targetTaskStats = new long[this.targetTasks.size()];
    }

    /**
     * no shuffle for better cache locality?
     */
    public void updateExtendedTargetId() {
        for (int e : partition_ratio.keySet()) {
            Double ratio = partition_ratio.get(e);
            int v = Math.max((int) (ratio * 0), 1);//run every 100000 times then turn.
            for (int i = 0; i < v; i++) {
                extendedTargetId.add(e);
            }
        }
//		Collections.shuffle(extendedTargetId);
        //	//LOG.DEBUG("extended target size:" + extendedTargetId.size());
    }

    public int chooseTasks(Object... values) {
        int taskId = 0;
        if (values.length > 0) {
            //String str = values.GetAndUpdate(0).toString(); // assume key is the first field
            String str = output_fields.select(input_fields, values).toString();
            int firstChoice = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % downTaskSize);
            int secondChoice = (int) (Math.abs(h2.hashBytes(str.getBytes()).asLong()) % downTaskSize);
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
            taskId = targetTasks[selected];
            targetTaskStats[selected]++;
        }
        return taskId;
    }

    public int chooseTasks(char[] values) {
        int taskId = 0;
        if (values.length > 0) {
            //String str = values.GetAndUpdate(0).toString(); // assume key is the first field
            String str = new String(values);
            int firstChoice = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % downTaskSize);
            int secondChoice = (int) (Math.abs(h2.hashBytes(str.getBytes()).asLong()) % downTaskSize);
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
            taskId = targetTasks[selected];
            targetTaskStats[selected]++;
        }
        return taskId;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param output
     * @return
     */
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param bid
     * @param output
     * @return
     */
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param bid
     * @param gap
     * @param output
     * @return
     */
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param output
     * @return
     */
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }

    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        int target = chooseTasks(key);
        try_offer(meta.src_id, target, streamId, key, value);
        return target;
    }

    public int emit_nowait(Meta meta, String streamId, char[] output) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
}
