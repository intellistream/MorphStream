package intellistream.morphstream.api.input.chc;


/**
 * APIs provided by the CHC module
 */
public class Api {
    public static void submit(Request request) {
        DBManager.getInstance().submit(request);
    }

    public static void submit(int requestId, int vertexId, int instanceId, int objKey, int objValue, Request.Operation operation, Pattern pattern) {
        Request request = Request.createRequest(requestId, vertexId, instanceId, objKey, objValue, operation, pattern);
        DBManager.getInstance().submit(request);
    }

    public static void submitReadRequest(int requestId, int vertexId, int instanceId, int objKey, Pattern pattern) {
        Request request = Request.createReadRequest(requestId, vertexId, instanceId, objKey, pattern);
        DBManager.getInstance().submit(request);
    }

    public static void submitWriteRequest(int requestId, int vertexId, int instanceId, int objKey, int objValue, Pattern pattern) {
        Request request = Request.createWriteRequest(requestId, vertexId, instanceId, objKey, objValue, pattern);
        DBManager.getInstance().submit(request);
    }
}
