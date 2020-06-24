package common.sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Marker;
public class MeasureSink_FT extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink_FT.class);
    private static final long serialVersionUID = 5481794109405775823L;
    @Override
    public void execute(JumboTuple input) {
        double results;
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            final Marker marker = input.getMarker(i);
            if (marker != null) {
                this.collector.ack(input, marker);
                continue;
            }
            results = helper.execute(input.getBID());
            if (results != 0) {
                this.setResults(results);
                LOG.info("Sink finished:" + results);
                if (thisTaskId == graph.getSink().getExecutorID()) {
                    measure_end(results);
                }
            }
        }
    }
}
