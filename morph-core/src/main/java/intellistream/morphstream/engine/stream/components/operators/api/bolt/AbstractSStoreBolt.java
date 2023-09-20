package intellistream.morphstream.engine.stream.components.operators.api.bolt;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerSStore;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;

import java.util.Map;


public abstract class AbstractSStoreBolt extends AbstractTransactionalBolt{

    public AbstractSStoreBolt(Logger log, int fid) {
        super(log, fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(),
                thread_Id, this.context.getThisComponent().getNumTasks());
        SOURCE_CONTROL.getInstance().config(tthread, 1);
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        MorphStreamEnv.get().databaseInitializer().loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), true);
    }
}
