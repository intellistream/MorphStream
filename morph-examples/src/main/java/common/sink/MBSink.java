package common.sink;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MBSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MBSink.class);
    private static final long serialVersionUID = 5481794109405775823L;
    int cnt = 0;

    @Override
    public void execute(Tuple input) {
        check(cnt, input);
        cnt++;
    }

    public void display() {
    }
}
