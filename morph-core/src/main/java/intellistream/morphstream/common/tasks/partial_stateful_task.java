package intellistream.morphstream.common.tasks;

import intellistream.morphstream.common.helper.Event;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by I309939 on 7/29/2016.
 */
public class partial_stateful_task extends stateful_task {
    private static final long serialVersionUID = -3206825365749743835L;
    final int SECTONANO = 1000000000;
    final int SECTOMS = 1000000;
    private final int in_core_complexity;
    private final int window;
    private HashMap<String, String> partial_map = new HashMap<>();
    private long timestamp = 0;

    public partial_stateful_task(int in_core_complexity, int window) {
        this.in_core_complexity = in_core_complexity;
        this.window = window;
        timestamp = System.nanoTime();
    }

    public HashMap<String, String> getPartial_map() {
        return partial_map;
    }

    public void setMap(HashMap<String, String> partial_map) {
        //this is used in Spark-streaming.
        this.partial_map = partial_map;
    }

    private HashMap<String, String> update_map(String key, String value) {
        if (partial_map.containsKey(key)) {
            partial_map.replace(key, value);
        } else {
            partial_map.put(key, value);
        }
        return partial_map;
    }

    public int execute_sparkEdition(Long time, String key, String value) {
        for (int i = 0; i < in_core_complexity; i++) {
            update_map(key, value);
        }
        return new Random().nextInt();
    }

    public int execute(Long time, String key, String value) {
        if ((time - timestamp) > (long) window * SECTONANO) {//a time window passed in ms.
            timestamp = time;
            partial_map.clear();
        }
        for (int i = 0; i < in_core_complexity; i++) {
            update_map(key, value);
        }
        return new Random().nextInt();
    }

    @Override
    public String execute(Long time, String key, String value, long stateful_process_start) {
        execute(time, key, value);
        if (stateful_process_start == 0) {
            return null;
        } else {
            final long function_process_end = System.nanoTime();
            return stateful_process_start + Event.split_expression + function_process_end;
        }
    }
}
