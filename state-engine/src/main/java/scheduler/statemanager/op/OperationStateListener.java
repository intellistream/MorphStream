package scheduler.statemanager.op;


import scheduler.struct.op.MetaTypes;
import scheduler.struct.op.MetaTypes.OperationStateType;
import scheduler.struct.op.Operation;

public interface OperationStateListener {
    void onOpParentExecuted(Operation operation, MetaTypes.DependencyType dependencyType, OperationStateType parentState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param headerState
     */
    void onOpNeedAbortHandling(Operation operation, OperationStateType headerState);

    /**
     * this method will only be used in header operation, others should not use it.
     *
     * @param operation
     * @param descendantState
     */
    void onHeaderStartAbortHandling(Operation operation, OperationStateType descendantState);

    /**
     * thread notify the operation execution results either success/failed, do state transition correspondingly
     *
     * @param operation
     */
    void onOpProcessed(Operation operation);

    void onRootStart(Operation operation);

    void onOpRollbackAndRedo(Operation operation, MetaTypes.DependencyType dependencyType, OperationStateType parentState, OperationStateType prevParentState);
    }
