package intellistream.morphstream.api.input.chc.common;

import lombok.Data;

/**
 * State Access Request
 */
@Data
public class Request {
    private final int requestId;
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

    public static Request createRequest(int requestId, int vertexId, int instanceId, int objKey, int objValue, Operation operation, Pattern pattern) {
        return new Request(requestId, vertexId, instanceId, objKey, objValue, operation, pattern);
    }

    public static Request createReadRequest(int requestId, int vertexId, int instanceId, int objKey, Pattern pattern) {
        return Request.createRequest(requestId, vertexId, instanceId, objKey, 0, Operation.READ, pattern);
    }

    public static Request createWriteRequest(int requestId, int vertexId, int instanceId, int objKey, int objValue, Pattern pattern) {
        return Request.createRequest(requestId, vertexId, instanceId, objKey, objValue, Operation.WRITE, pattern);
    }
}

