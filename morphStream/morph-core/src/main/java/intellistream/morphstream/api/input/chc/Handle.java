package intellistream.morphstream.api.input.chc;

import lombok.Getter;

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
