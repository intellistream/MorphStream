package java.intellistream.chc.database.manage.handler.impl;

import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.database.manage.DBManager;
import java.intellistream.chc.database.manage.handler.Handler;

/**
 * Cross-flow state access handler
 */
public class CrossflowHandler extends Handler {
    public CrossflowHandler(Request request) {
        super(request);
    }

    @Override
    public void run() {
        if (request.getOperation() == Request.Operation.WRITE) {
            DBManager.getInstance().getDatabase().putSharedState(request.getVertexId(), request.getObjKey(), request.getObjValue());
            // TODO: update the cache
        } else {
            Object state = DBManager.getInstance().getDatabase().findSharedState(request.getVertexId(), request.getObjKey());
            // TODO: send the state to the requester
        }
    }
}
