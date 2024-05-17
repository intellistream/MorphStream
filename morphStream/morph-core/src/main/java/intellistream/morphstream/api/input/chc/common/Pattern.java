package intellistream.morphstream.api.input.chc.common;

import lombok.Data;

/**
 * There exists four types of access patterns in the CHC system.
 */
@Data
public class Pattern {
    private final StateType stateType;
    private final AccessType accessType;

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


