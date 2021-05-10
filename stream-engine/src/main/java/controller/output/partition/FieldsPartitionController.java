package controller.output.partition;
import common.collections.Configuration;
import common.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.TopologyComponent;
import controller.output.PartitionController;
import controller.output.partition.impl.TupleUtils;
import execution.ExecutionNode;
import execution.runtime.collector.impl.Meta;
import execution.runtime.tuple.impl.Fields;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public class FieldsPartitionController extends PartitionController {
    private static final long serialVersionUID = -6238113480852616028L;
    private static final Logger LOG = LoggerFactory.getLogger(FieldsPartitionController.class);
    protected final Fields input_fields;
    final Fields output_fields;
    private final int targetTasksize;
    public FieldsPartitionController(TopologyComponent operator, TopologyComponent childOP,
                                     HashMap<Integer, ExecutionNode> executionNodeHashMap,
                                     Fields output_fields, Fields input_fields, int batch_size, ExecutionNode executor, boolean profile, Configuration conf) {
        super(operator, childOP, executionNodeHashMap, batch_size, executor, LOG, profile, conf);
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        targetTasksize = setID.size();
        targetTasks = setID.toArray(new Integer[setID.size()]);
        this.output_fields = output_fields;
        this.input_fields = input_fields;
    }
    public int chooseTasks(Object... values) {
//		int targetTaskIndex = TupleUtils.chooseTaskIndex(outFields.select(groupFields, values), numTasks);
//		return targetTasks.GetAndUpdate(targetTaskIndex);
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(Object values) {
//		int targetTaskIndex = TupleUtils.chooseTaskIndex(outFields.select(groupFields, values), numTasks);
//		return targetTasks.GetAndUpdate(targetTaskIndex);
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(StreamValues values) {
//		int targetTaskIndex = TupleUtils.chooseTaskIndex(outFields.select(groupFields, values), numTasks);
//		return targetTasks.GetAndUpdate(targetTaskIndex);
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(char[] values) {
//		int targetTaskIndex = TupleUtils.chooseTaskIndex(outFields.select(groupFields, values), numTasks);
//		return targetTasks.GetAndUpdate(targetTaskIndex);
        int targetTaskIndex = TupleUtils.chooseTaskIndex(Arrays.hashCode(values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(int values) {
//		int targetTaskIndex = TupleUtils.chooseTaskIndex(outFields.select(groupFields, values), numTasks);
//		return targetTasks.GetAndUpdate(targetTaskIndex);
        int targetTaskIndex = TupleUtils.chooseTaskIndex(Integer.hashCode(values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, msg_id, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        int target = chooseTasks(deviceID);
        offer(meta.src_id, target, streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, bid, key, value);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, key, value);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, key, value, bid, TimeStamp);
        return target;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        int target = chooseTasks(output);
        offer_inorder_single(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    //    @Override
//    public int emit_marked(String streamId, Object... output, long timestamp, long bid) {
//        /**
//         * TODO: % is too slow, need some better ways to implement faster hash.
//         * Further, it causes key skew problem.
//         */
//
//        int hashcode = Math.abs(output_fields.select(output, input_fields).hashCode());
//        int target = targetTasks[hashcode % targetTasksize];
//        offer_marked(target, streamId, output, timestamp, msgID);
//        return target;
//    }
//
//    @Override
//    public int emit_marked(String streamId, Object... output, Marker marker) {
//        /**
//         * TODO: % is too slow, need some better ways to implement faster hash.
//         * Further, it causes key skew problem.
//         */
//
//        int hashcode = Math.abs(output_fields.select(output, input_fields).hashCode());
//        int target = targetTasks[hashcode % targetTasksize];
//        offer_marked(target, streamId, output, marker.timeStampNano, marker.msgId);
//        return target;
//    }
    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        int target = chooseTasks(key);
        try_offer(meta.src_id, target, streamId, key, value);
        return target;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) {
        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
}
