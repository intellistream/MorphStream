package java.intellistream.chc.common.dao;

/**
 * There exists four types of access patterns in the CHC system.
 */
public class Pattern {
    public final StateType stateType;
    public final AccessType accessType;

    public Pattern(StateType stateType, AccessType accessType) {
        this.stateType = stateType;
        this.accessType = accessType;
    }

    /**
     * The type of the state
     */
    public enum StateType {
        PER_FLOW,
        CROSS_FLOW,
    }

    /**
     * The access types
     */
    public enum AccessType {
        READ,
        WRITE,
        READ_WRITE,
    }
}


