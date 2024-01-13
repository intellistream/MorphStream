package java.intellistream.chc.common.dao;

import lombok.Data;

/**
 * State Access Request
 */
@Data
public class Request {
    private final int vertexId;
    private final int instanceId;
    private final int objKey;
    private final int objValue;
    private final Operation operation;
    private final Pattern pattern;

    public enum Operation {
        READ,
        WRITE
    }
}
