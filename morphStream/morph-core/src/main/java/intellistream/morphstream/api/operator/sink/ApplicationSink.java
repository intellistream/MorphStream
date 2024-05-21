package intellistream.morphstream.api.operator.sink;

import intellistream.morphstream.engine.stream.components.operators.api.sink.AbstractSink;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class ApplicationSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSink.class);
    public ApplicationSink(String id, int fid) {
        super(id, LOG, fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        if (in.isMarker()) {
//            LOG.info("Received marker" + in.getBID());
        }
    }

}
