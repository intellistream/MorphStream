package intellistream.morphstream.engine.txn.scheduler.impl.ds;

import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.TaskPrecedenceGraph;
import org.apache.log4j.Logger;

public abstract class DSSchedule<Context extends DSContext> implements IScheduler<Context> {
    private static final Logger LOG = Logger.getLogger(DSSchedule.class);
    public TaskPrecedenceGraph<Context> tpg;
}
