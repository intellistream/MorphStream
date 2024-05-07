package java.intellistream.chc.database.manage.handle.impl;

import java.intellistream.chc.NativeInterface;
import java.intellistream.chc.common.dao.Request;
import java.intellistream.chc.database.manage.DBManager;
import java.intellistream.chc.database.manage.handle.Handle;

/**
 * Cross-flow state access handler
 */
public class CrossflowHandle extends Handle {
    public CrossflowHandle(Request request) {
        super(request);
    }

    @Override
    public void run() {
        if (request.getOperation() == Request.Operation.WRITE) {
            DBManager.getInstance().getDatabase().putSharedState(request.getVertexId(), request.getObjKey(), request.getObjValue());
            NativeInterface.__update_cache(request.getObjKey(), request.getObjValue()); // notify the threads updating the cache
        } else {
            // READ
            int state = DBManager.getInstance().getDatabase().findSharedState(request.getVertexId(), request.getObjKey());
            NativeInterface.__return_state(request.getRequestId(), state);    // return the state back to the requester
        }
    }
}