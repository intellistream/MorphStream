package transaction.scheduler.tpg.signal.oc;

import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.OperationChain;

import javax.annotation.Nullable;

public class OnRootSignal extends OperationChainSignal {
    public OnRootSignal(OperationChain targetOperationChain) {
        super(targetOperationChain);
    }
}
