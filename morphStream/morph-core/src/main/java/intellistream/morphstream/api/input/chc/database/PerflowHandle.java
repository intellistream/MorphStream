package intellistream.morphstream.api.input.chc.database;

import java.intellistream.chc.NativeInterface;
import java.intellistream.chc.common.dao.Request;

/**
 * Per-flow state access handler
 */
public class PerflowHandle extends Handle {
    public PerflowHandle(Request request) {
        super(request);
    }

    @Override
    public void run() {
        if (request.getOperation() == Request.Operation.WRITE) {
            DBManager.getInstance().getDatabase().putExclusiveState(request.getVertexId(), request.getInstanceId(), request.getObjKey(), request.getObjValue());
        } else {
            // return the read state to the requester
            int state = DBManager.getInstance().getDatabase().findExclusiveState(request.getVertexId(), request.getInstanceId(), request.getObjKey());
            NativeInterface.__return_state(request.getRequestId(), state);
        }
    }
}
