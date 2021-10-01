package scheduler.oplevel.impl.tpg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.oplevel.context.OPLayeredContext;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;
import scheduler.struct.layered.dfs.DFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.Collection;

public class OPDFSScheduler<Context extends OPLayeredContext> extends OPLayeredScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPDFSScheduler.class);


    public OPDFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    /**
     * // O1 -> (logical)  O2
     * // T1: pickup O1. Transition O1 (ready - > execute) || notify O2 (speculative -> ready).
     * // T2: pickup O2 (speculative -> executed)
     * // T3: pickup O2
     * fast explore dependencies in TPG and put ready/speculative operations into task queues.
     *
     * @param context
     */
    @Override
    public void EXPLORE(Context context) {
        Operation op = Next(context);
        while (op == null) {
            if (context.finished())
                break;
            ProcessedToNextLevel(context);
            op = Next(context);
        }
        while (op != null && op.hasParents()) ;
        DISTRIBUTE(op, context);
    }

    /**
     * notify is handled by state manager of each thread
     *
     * @param operation
     * @param context
     */
    @Override
    protected void NOTIFY(Operation operation, Context context) {
        operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
        Collection<Operation> ops = operation.getChildren(MetaTypes.DependencyType.TD);
        for (Operation childOP : ops) {
            childOP.updateDependencies(MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.EXECUTED);
        }
        ops = operation.getChildren(MetaTypes.DependencyType.FD);
        for (Operation childOP : ops) {
            childOP.updateDependencies(MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }
        ops = operation.getChildren(MetaTypes.DependencyType.LD);
        for (Operation childOP : ops) {
            childOP.updateDependencies(MetaTypes.DependencyType.LD, MetaTypes.OperationStateType.EXECUTED);
        }
    }
}