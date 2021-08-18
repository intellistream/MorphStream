package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.oc.OnExecutedSignal;
import transaction.scheduler.tpg.signal.oc.OnParentExecutedSignal;
import transaction.scheduler.tpg.signal.oc.OnRootSignal;
import transaction.scheduler.tpg.signal.oc.OperationChainSignal;
import transaction.scheduler.tpg.signal.op.*;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager {
    public final ArrayList<String> partition; //  list of states being responsible for

    public PartitionStateManager() {
        this.partition = new ArrayList<>();
    }
}
