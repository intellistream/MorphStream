package intellistream.morphstream.api.input.chc.database;

import lombok.Getter;

import java.intellistream.chc.common.dao.Request;

/**
 * Abstract handler
 */
@Getter
public abstract class Handle implements Runnable {
    public final Request request;

    public Handle(Request request) {
        this.request = request;
    }
}
