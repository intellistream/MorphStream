package common.bolts.wc;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.WordCountConstants.Field;
import common.util.datatypes.StreamValues;
import components.operators.api.Checkpointable;
import components.operators.base.MapBolt;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.impl.ValueState;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt_FT extends MapBolt implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_FT.class);
    private static final long serialVersionUID = 8264005289694109091L;
    //private int total_thread=context.getThisTaskId();
//    private static final String splitregex = " ";
//    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<String, MutableLong> counts = new HashMap<>();

    public WordCountBolt_FT() {
        super(LOG);
        state = new ValueState();
    }

    public Integer default_scale(Configuration conf) {
        return conf.getInt("num_socket") * 15;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        final long bid = in.getBID();
        String word = in.getStringByField(Field.WORD);
        MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
        count.increment();
        StreamValues value = new StreamValues(word, count.longValue());
        collector.emit(bid, value);
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            final Marker marker = in.getMarker(i);
            if (marker != null) {
                //LOG.DEBUG(this.getContext().getThisTaskId() + " receive marker:" + marker.msgId + "from " + in.getSourceTask());
                forward_checkpoint(in.getSourceTask(), bid, marker);
                continue;
            }
            String word = in.getString(0, i);
            MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
            count.increment();
            StreamValues value = new StreamValues(word, count.longValue());
            collector.emit(bid, value);
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID();
//		LOG.info("PID  = " + pid);
    }

    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) {
    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {
    }

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        //(Serializable) counts checkpoint_forward(sourceId);
        final boolean check = checkpoint_store((Serializable) counts, sourceId, marker);//call forward_checkpoint.
        if (check) {
            this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
            //LOG.DEBUG(this.getContext().getThisComponentId() + this.getContext().getThisTaskId() + " broadcast marker with id:" + marker.msgId + "@" + DateTime.now());
        }
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) {
    }

    @Override
    public void ack_checkpoint(Marker marker) {
//		//LOG.DEBUG(this.getContext().getThisTaskId() + " received ack from all consumers.");
        //Do something to clear past state. (optional)
//		//LOG.DEBUG(this.getContext().getThisTaskId() + " broadcast ack to all producers.");
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {
    }
}
