package java.intellistream.chc.database.manage.handler;

import java.intellistream.chc.common.dao.Request;

/**
 * Abstract handler
 */
public abstract class Handler implements Runnable {
    public final Request request;

    public Handler(Request request) {
        this.request = request;
    }
}
