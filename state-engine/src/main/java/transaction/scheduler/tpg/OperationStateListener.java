package transaction.scheduler.tpg;


import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;

public interface OperationStateListener {
    void onOpParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param headerState
     */
    void onOpHeaderStateUpdated(Operation operation, MetaTypes.OperationStateType headerState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param descendantState
     */
    void onOpDescendantStateUpdated(Operation operation, MetaTypes.OperationStateType descendantState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param child
     * @param spLd
     * @param executed
     */
    void onReadyParentStateUpdated(Operation child, MetaTypes.DependencyType spLd, MetaTypes.OperationStateType executed);
}
