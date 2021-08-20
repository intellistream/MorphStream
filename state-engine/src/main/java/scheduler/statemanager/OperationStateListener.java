package scheduler.statemanager;


import scheduler.struct.MetaTypes;
import scheduler.struct.bfs.BFSOperation;

public interface OperationStateListener {
    void onOpParentStateUpdated(BFSOperation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param headerState
     */
    void onOpHeaderStateUpdated(BFSOperation operation, MetaTypes.OperationStateType headerState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param descendantState
     */
    void onOpDescendantStateUpdated(BFSOperation operation, MetaTypes.OperationStateType descendantState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param child
     * @param spLd
     * @param executed
     */
    void onReadyParentStateUpdated(BFSOperation child, MetaTypes.DependencyType spLd, MetaTypes.OperationStateType executed);
}
