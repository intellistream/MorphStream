package scheduler.oplevel.struct;

public interface MetaTypes {

    enum DependencyType {
        FD, TD, LD, SP_LD
    }

    /**
     * Operation state for fine-grained scheduling
     */
    enum OperationStateType {
        BLOCKED,
        READY,
        SPECULATIVE,
        EXECUTED,
        ABORTED,
        COMMITTABLE,
        COMMITTED
    }
}
