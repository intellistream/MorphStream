package common.tasks;

import common.helper.Event;
import common.tools.KB_object;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;

import static common.CONTROL.enable_log;

/**
 * Created by I309939 on 7/30/2016.
 */
public class fully_stateful_task extends stateful_task {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(fully_stateful_task.class);
    private static final long serialVersionUID = 3072579886348914507L;
    final private int size_state;
    private final int in_core_complexity;
    protected ArrayList<KB_object> list = new ArrayList<>();

    public fully_stateful_task(int in_core_complexity, int size_state) {
        this.in_core_complexity = in_core_complexity;
        this.size_state = size_state;
        if (enable_log)
            LOG.warn(Thread.currentThread().getName() + ":" + "Fully stateful task with state size:" + size_state);
    }

    public ArrayList<KB_object> getMap() {
        return list;
    }

    public void setMap(ArrayList<KB_object> list) {
        this.list = list;
    }

    private void update_map() {
        Random r = new Random();
        list.set(r.nextInt(size_state), new KB_object());
    }

    @Override
    public int execute(Long time, String key, String value) {
        if (list.size() == 0) {
            list = KB_object.create_myObjectList(size_state);
        }
        for (int i = 0; i < in_core_complexity; i++) {
            update_map();
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
