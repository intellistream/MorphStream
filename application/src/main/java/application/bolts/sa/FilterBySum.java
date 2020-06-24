package application.bolts.sa;

import application.Constants;
import application.constants.streamingAnalysisConstants.Field;
import application.util.datatypes.StreamValues;
import sesame.components.context.TopologyContext;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FilterBySum extends filterBolt {
    private static final String split_expression = ",";
    private static final Logger LOG = LoggerFactory.getLogger(FilterBySum.class);
    private static final long serialVersionUID = 2101229570261223498L;
    double cnt = 0;
    double cnt1 = 0;

    private FilterBySum() {
        super(LOG, new HashMap<>());//it's configurable..
        this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 0.9);
    }


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIME, Field.VALUE);
    }

    private double sumInValue(String value) {
        double sum = 0;
        String[] split = value.split(split_expression);
        for (String item : split) {
            // long size_of_item = MemoryUtil.deepMemoryUsageOf(item);
            sum += Double.parseDouble(item);
        }
        return sum;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        final Long time = in.getLong(0);
        final String key = in.getString(1);
        final String value = in.getString(2);
        double sumInValue = sumInValue(value);
        final long bid = in.getBID();
//        cnt++;

        if (sumInValue > 0) {//-1:100%; 0: 90%
            final StreamValues objects =
                    new StreamValues(time, key, value);
            //StableValues.create(time, key, value_list);//a memory write happens here (could be in cache..)
            collector.emit(bid, objects);//emit the tuple reference.
//            cnt1++;
        }
//        double i = cnt1 / cnt;
//        assert i > 0;
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            //if (stat != null) stat.start_measure();
            final Long time = in.getLong(0, i);
            final String key = in.getString(1, i);
            final String value = in.getString(2, i);
            double sumInValue = sumInValue(value);
            //cnt++;
            if (sumInValue > 0) {//-1:100%; 0: 90%
                final StreamValues objects =
                        new StreamValues(time, key, value);
                //StableValues.create(time, key, value_list);//a memory write happens here (could be in cache..)
                collector.emit(bid, objects);//emit the tuple reference.
                //cnt1++;
            }
        }
    }
}
