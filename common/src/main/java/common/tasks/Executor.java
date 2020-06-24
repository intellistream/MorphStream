package common.tasks;
import common.helper.Event;
/**
 * Created by I309939 on 8/3/2016.
 */
public class Executor {
    final stateless_task mystatelesstask;
    final stateful_task mytask;
    public Executor(stateless_task mystatelesstask, stateful_task mytask) {
        this.mystatelesstask = mystatelesstask;
        this.mytask = mytask;
    }
    /**
     * Used in normal execution
     *
     * @param time
     * @param key
     * @param value
     * @return
     */
    public int execute(Long time, String key, String value) {
        int result = mystatelesstask.execute(value);
        if (mytask != null) {
            result += mytask.execute(time, key, value);
        }
        return result;
    }
    /**
     * Used in verbose execution
     *
     * @param time
     * @param key
     * @param value
     * @param function_process_start
     * @return
     */
    public String execute(Long time, String key, String value,
                          long function_process_start) {
        if (function_process_start == 0) {
            execute(time, key, value);
            return null;
        } else {
            final String stateless_time = mystatelesstask.execute(value, function_process_start);
            String stateful_time = null;
            if (mytask != null) {
                final long stateful_process_start = System.nanoTime();
                stateful_time = mytask.execute(time, key, value, stateful_process_start);
            }
            if (stateful_time != null)
                return "stateless" + Event.split_expression + stateless_time + Event.split_expression
                        + "stateful" + Event.split_expression + stateful_time;
            return "stateless" + Event.split_expression + stateless_time;
        }
    }
}
