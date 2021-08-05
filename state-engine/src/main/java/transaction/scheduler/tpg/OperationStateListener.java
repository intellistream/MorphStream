package transaction.scheduler.tpg;


import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;

public interface OperationStateListener {
    void onParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState);

    /**
     * this method will only be used in header operation, others should not use it.
     * @param operation
     * @param headerState
     */
    void onHeaderStateUpdated(Operation operation, MetaTypes.OperationStateType headerState);

    /**
     * this method will only be used in header operation, others should not use it.
     * @param operation
     * @param descendantState
     */
    void onDescendantStateUpdated(Operation operation, MetaTypes.OperationStateType descendantState);

    /**
     * thread notify the operation execution results either success/failed, do state transition correspondingly
     * @param operation
     * @param isFailed
     */
    void onProcessed(Operation operation, boolean isFailed);

    /**
     * this method will only be used in header operation, others should not use it.
     * @param child
     * @param spLd
     * @param executed
     */
    void onReadyParentStateUpdated(Operation child, MetaTypes.DependencyType spLd, MetaTypes.OperationStateType executed);
}
