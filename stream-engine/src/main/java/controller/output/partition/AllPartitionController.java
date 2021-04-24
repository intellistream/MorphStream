package controller.output.partition;
import common.collections.Configuration;
import common.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.TopologyComponent;
import controller.output.PartitionController;
import execution.ExecutionNode;
import execution.runtime.collector.impl.Meta;
import execution.runtime.tuple.impl.Fields;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public class AllPartitionController extends PartitionController {
    private static final long serialVersionUID = 245951208021313536L;
    private static Logger LOG = LoggerFactory.getLogger(AllPartitionController.class);
    private final int downExecutor_size;
    private Fields fields;
    public AllPartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer, ExecutionNode> executionNodeHashMap
            , int batch, ExecutionNode executor, boolean common, boolean profile, Configuration conf) {
        super(operator, childOP, executionNodeHashMap, batch, executor, common, LOG, profile, conf);
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        downExecutor_size = setID.size();
        targetTasks = setID.toArray(new Integer[setID.size()]);
        this.fields = fields;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            offer_bid(meta.src_id, target, streamId, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        for (int target : targetTasks) {
            offer_bid(meta.src_id, target, streamId, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        for (int target : targetTasks) {
            offer_bid(meta.src_id, target, streamId, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            force_offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            force_offer(meta.src_id, target, streamId, msg_id, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) {
        throw new UnsupportedOperationException();
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) {
        throw new UnsupportedOperationException();
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, key, value);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, key, value);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, key, value, bid, TimeStamp);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) {
        for (int target : targetTasks) {
            offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) {
        for (int target : targetTasks) {
            offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        for (int target : targetTasks) {
            offer_inorder(meta.src_id, target, streamId, bid, gap, tuple);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        for (int target : targetTasks) {
            offer_inorder_single(meta.src_id, target, streamId, bid, gap, tuple);
        }
        return downExecutor_size;
    }
    //    @Override
//    public int emit_marked(String streamId, Object... output, long timestamp, long msgID) {
//        for (int target : targetTasks) {
//            offer_marked(target, streamId, output, timestamp, msgID);
//        }
//        return downExecutor_size;
//    }
//
//    @Override
//    public int emit_marked(String streamId, Object... output, Marker marker) {
//        for (int target : targetTasks) {
//            offer_marked(target, streamId, output, marker.timeStampNano, marker.msgId);
//        }
//        return downExecutor_size;
//    }
    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        for (int target : targetTasks) {
            try_offer(meta.src_id, target, streamId, output);
        }
        //int target = targetTasks[hashcode % downExecutor_size];
        //  try_offer(target, tuple);
        return downExecutor_size;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        for (int target : targetTasks) {
            try_offer(meta.src_id, target, streamId, key, value);
        }
        //int target = targetTasks[hashcode % downExecutor_size];
        //  try_offer(target, tuple);
        return downExecutor_size;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) {
        for (int target : targetTasks) {
            try_offer(meta.src_id, target, streamId, output);
        }
        //int target = targetTasks[hashcode % downExecutor_size];
        //  try_offer(target, tuple);
        return downExecutor_size;
    }
}
