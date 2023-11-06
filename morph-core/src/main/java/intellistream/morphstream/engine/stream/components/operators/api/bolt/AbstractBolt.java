package intellistream.morphstream.engine.stream.components.operators.api.bolt;

import intellistream.morphstream.engine.stream.components.operators.api.Operator;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public abstract class AbstractBolt extends Operator {
    public AbstractBolt(String id, Logger log, int fid) {
        super(id, log, null, null, 1, 1, 1);
        this.fid = fid;
    }
    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            execute(new Tuple(in.getBID(), in.getSourceTask(), in.getContext(), in.msg[i]));
        }
    }
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;
}
