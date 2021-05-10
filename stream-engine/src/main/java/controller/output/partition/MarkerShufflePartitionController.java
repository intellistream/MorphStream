package controller.output.partition;
import common.collections.Configuration;
import common.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.TopologyComponent;
import execution.ExecutionNode;
import execution.runtime.collector.impl.Meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
/**
 * Created by shuhaozhang on 11/7/16.
 * Every stream corresponds a partitionController, who owns the output queue for each downstream executor.
 */
public class MarkerShufflePartitionController extends ShufflePartitionController {
    private static final long serialVersionUID = 8345435833774292214L;
    private static Logger LOG = LoggerFactory.getLogger(MarkerShufflePartitionController.class);
    private final int marker_sink;//the last sink.
    ArrayList<Integer> extendedTargetId = new ArrayList<>();
    public MarkerShufflePartitionController(
            TopologyComponent operator, TopologyComponent childOP, int sinkID, HashMap<Integer, ExecutionNode> downExecutor_list, int batch, ExecutionNode executor, boolean common, boolean profile, Configuration conf) {
        super(operator, childOP, downExecutor_list, batch, executor, LOG, profile, conf);
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        targetTasks = setID.toArray(new Integer[setID.size()]);
        updateExtendedTargetId();
        marker_sink = sinkID;
    }
    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param output
     * @return
     */
    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        offer_bid(meta.src_id, marker_sink, streamId, output);
        return marker_sink;
    }
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        offer_bid(meta.src_id, marker_sink, streamId, output);
        return marker_sink;
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
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        offer(meta.src_id, marker_sink, streamId, bid, output);
        return marker_sink;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        offer(meta.src_id, marker_sink, streamId, bid, output);
        return marker_sink;
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
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) {
        offer_inorder(meta.src_id, marker_sink, streamId, bid, gap, output);
        return marker_sink;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        offer_inorder(meta.src_id, marker_sink, streamId, bid, gap, output);
        return marker_sink;
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
        try_offer(meta.src_id, marker_sink, streamId, output);
        return marker_sink;
    }
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        try_offer(meta.src_id, marker_sink, streamId, key, value);
        return marker_sink;
    }
    public int emit_nowait(Meta meta, String streamId, char[] output) {
        try_offer(meta.src_id, marker_sink, streamId, output);
        return marker_sink;
    }
}
