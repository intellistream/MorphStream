package intellistream.morphstream.engine.stream.components.operators.api.bolt;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;

import java.util.Map;

import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;


public abstract class AbstractMorphStreamBolt extends AbstractTransactionalBolt {
    protected Object input_event;
    protected long timestamp;
    protected long _bid;
    public AbstractMorphStreamBolt(Logger log, int fid) {
        super(log, fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(),
                thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler"));
        if (config.getBoolean("isGroup")) {
            SOURCE_CONTROL.getInstance().config(tthread, config.getInt("groupNum"));
        } else {
            SOURCE_CONTROL.getInstance().config(tthread, 1);
        }
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        MorphStreamEnv.get().databaseInitializer().loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), false);
    }
    protected abstract void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException;
    protected abstract void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException;
    protected abstract void PRE_EXECUTE(Tuple in);
}
