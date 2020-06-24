package common.tasks;
import common.helper.Event;

import static java.lang.Math.toIntExact;
public class stateless_taskImpl extends stateless_task {
    private static final long serialVersionUID = -3037968572420543072L;
    private final int in_core_complexity;
    private final int off_core_complexity;
    //private final int number_objects = 20000000;
    //myObject[] array = new myObject[number_objects];
    public stateless_taskImpl(int task_complexity, int off_core_complexity) {
        this.in_core_complexity = task_complexity;
        this.off_core_complexity = off_core_complexity;
    }
    /**
     * used in normal call
     *
     * @param value
     * @return
     */
    public int execute(String value) {
        int sum = 0;
        //random_compute
        for (int i = 0; i < this.in_core_complexity; i++) {
            //sum = 0;
            sum += random_compute(value);
        }
        //random_memory
        for (int i = 0; i < this.off_core_complexity; i++) {
            sum = sum + toIntExact(showLong());
        }
        return sum;
    }
    /**
     * used in verbose call
     *
     * @param value
     * @param function_process_start
     * @return
     */
    public String execute(String value, long function_process_start) {
        execute(value);
        final long function_process_end = System.nanoTime();
        if (function_process_start == 0) {
            return null;
        }
        return function_process_start + Event.split_expression + function_process_end;
    }
}
