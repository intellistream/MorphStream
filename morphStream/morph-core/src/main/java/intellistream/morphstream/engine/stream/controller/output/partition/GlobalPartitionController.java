package intellistream.morphstream.engine.stream.controller.output.partition;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.controller.output.PartitionController;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.impl.Meta;
import intellistream.morphstream.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by shuhaozhang on 11/7/16.
 * Every stream corresponds a partitionController, who owns the output queue for each downstream executor.
 * <profiling>
 * GLobal Partition is likely to be a bottleneck as the downstream Aoperator has only one executor.
 */
public class GlobalPartitionController extends PartitionController {
    private static final long serialVersionUID = -8130612976775054066L;
    private static final Logger LOG = LoggerFactory.getLogger(GlobalPartitionController.class);

    /*Global partition must have only one downstream executor*/
    public GlobalPartitionController(TopologyComponent operator, TopologyComponent childOP,
                                     HashMap<Integer, ExecutionNode> executionNodeHashMap, int batch, ExecutionNode executor, boolean profile, Configuration conf) {
        super(operator, childOP, executionNodeHashMap, batch, executor, LOG, profile, conf);
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        targetTasks = setID.toArray(new Integer[setID.size()]);
    }

    public int emit(Meta meta, String streamId, long bid, Object... tuple) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, Object tuple) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, tuple);
        return targetTasks[0];
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        force_offer(meta.src_id, targetTasks[0], streamId, bid, output);
        return targetTasks[0];
    }

    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        force_offer(meta.src_id, targetTasks[0], streamId, msg_id, bid, output);
        return targetTasks[0];
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        force_offer(meta.src_id, targetTasks[0], streamId, bid, output);
        return targetTasks[0];
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        force_offer(meta.src_id, targetTasks[0], streamId, bid, output);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, output);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        return targetTasks[0];
    }

    public int emit(Meta meta, String streamId, long bid, char[] tuple) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, bid, key, value);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, key, value);
        return targetTasks[0];
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        offer(meta.src_id, targetTasks[0], streamId, key, value, bid, TimeStamp);
        return targetTasks[0];
    }

    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... tuple) {
        offer_inorder(meta.src_id, targetTasks[0], streamId, bid, gap, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] tuple) {
        offer_inorder(meta.src_id, targetTasks[0], streamId, bid, gap, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        offer_inorder(meta.src_id, targetTasks[0], streamId, bid, gap, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        offer_inorder_single(meta.src_id, targetTasks[0], streamId, bid, gap, tuple);
        return targetTasks[0];
    }

    public int emit_bid(Meta meta, String streamId, Object... tuple) throws InterruptedException {
        offer_bid(meta.src_id, targetTasks[0], streamId, tuple);
        return targetTasks[0];
    }

    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        offer_bid(meta.src_id, targetTasks[0], streamId, output);
        return targetTasks[0];
    }

    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        offer_bid(meta.src_id, targetTasks[0], streamId, output);
        return targetTasks[0];
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        try_offer(meta.src_id, targetTasks[0], streamId, output);
        return targetTasks[0];
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        try_offer(meta.src_id, targetTasks[0], streamId, key, value);
        return targetTasks[0];
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) {
        try_offer(meta.src_id, targetTasks[0], streamId, output);
        return targetTasks[0];
    }
}
