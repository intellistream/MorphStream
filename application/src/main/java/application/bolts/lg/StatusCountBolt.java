package application.bolts.lg;

import application.constants.LogProcessingConstants.Field;
import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountBolt.class);
    private static final long serialVersionUID = 5959282247603126565L;
    private Map<Integer, Integer> counts;

    public StatusCountBolt() {
        super(LOG, 0.21);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        this.counts = new HashMap<>();
        LOG.info(Thread.currentThread().getName());
        this.scalable = false;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        int statusCode = in.getIntegerByField(Field.RESPONSE);
        int count = 0;
        final long bid = in.getBID();
        if (counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }

        count++;
        counts.put(statusCode, count);


        collector.emit(bid, new StreamValues(statusCode, count));

//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            int statusCode = in.getIntegerByField(Field.RESPONSE, i);
            int count = 0;
            if (counts.containsKey(statusCode)) {
                count = counts.get(statusCode);
            }

            count++;
            counts.put(statusCode, count);
            collector.emit(bid, new StreamValues(statusCode, count));
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RESPONSE, Field.COUNT);
    }
}
