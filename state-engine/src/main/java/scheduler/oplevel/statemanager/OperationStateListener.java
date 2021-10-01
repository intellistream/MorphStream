package scheduler.oplevel.statemanager;


import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

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
     * thread notify the operation execution results either success/failed, do state transition correspondingly
     *
     * @param operation
     */
    void onOpProcessed(Operation operation);

    void onRootStart(Operation operation);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param child
     * @param spLd
     * @param executed
     */
    void onOpReadyParentStateUpdated(Operation child, MetaTypes.DependencyType spLd, MetaTypes.OperationStateType executed);
}
