package application.bolts.lg;

import application.constants.LogProcessingConstants.Conf;
import application.constants.LogProcessingConstants.Field;
import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt will count number of log events per minute
 */
public class VolumeCountBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VolumeCountBolt.class);
    private static final long serialVersionUID = -1647515231750775653L;

    private CircularFifoBuffer buffer;
    private Map<Long, MutableLong> counts;

    public VolumeCountBolt() {
        super(LOG, 0.09);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int windowSize = config.getInt(Conf.VOLUME_COUNTER_WINDOW, 60);
        buffer = new CircularFifoBuffer(windowSize);
        counts = new HashMap<>(windowSize);
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        long minute = in.getLongByField(Field.TIMESTAMP_MINUTES);

        MutableLong count = counts.get(minute);

        if (count == null) {
            if (buffer.isFull()) {
                long oldMinute = (Long) buffer.remove();
                counts.remove(oldMinute);
            }
            count = new MutableLong(1);
            counts.put(minute, count);
            buffer.add(minute);
        } else {
            count.increment();
        }

        collector.emit(bid, new StreamValues(minute, count.longValue()));

//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

            long minute = in.getLongByField(Field.TIMESTAMP_MINUTES, i);

            MutableLong count = counts.get(minute);

            if (count == null) {
                if (buffer.isFull()) {
                    long oldMinute = (Long) buffer.remove();
                    counts.remove(oldMinute);
                }
                count = new MutableLong(1);
                counts.put(minute, count);
                buffer.add(minute);
            } else {
                count.increment();
            }

            collector.emit(bid, new StreamValues(minute, count.longValue()));

        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP_MINUTES, Field.COUNT);
    }
}
