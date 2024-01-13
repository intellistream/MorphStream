package java.intellistream.chc.database.manage.handler.impl;

import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.database.manage.DBManager;
import java.intellistream.chc.database.manage.handler.Handler;

/**
 * Per-flow state access handler
 */
public class PerflowHandler extends Handler {
    public PerflowHandler(Request request) {
        super(request);
    }

    @Override
    public void run() {
        if (request.getOperation() == Request.Operation.WRITE) {
            DBManager.getInstance().getDatabase().putExclusiveState(request.getVertexId(), request.getInstanceId(), request.getObjKey(), request.getObjValue());
        } else {
            Object state = DBManager.getInstance().getDatabase().findExclusiveState(request.getVertexId(), request.getInstanceId(), request.getObjKey());
            // TODO: send the state to the requester
        }
    }
}
