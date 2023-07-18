package common.tasks;

import java.io.Serializable;

/**
 * Created by I309939 on 7/30/2016.
 */
public abstract class stateful_task implements Serializable {
    private static final long serialVersionUID = 10L;

    public abstract int execute(Long time, String key, String value);

    public abstract String execute(Long time, String key, String value, long stateful_process_start);
}
