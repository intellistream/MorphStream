package application.constants;

public interface S_STOREConstants extends BaseConstants {
    String PREFIX = "ss";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConf {
        String INSERTOR_THREADS = "ss.insert.threads";
        String SELECTOR_THREADS = "ss.selector.threads";
        String Executor_Threads = "ss.executor.threads";
    }

    interface Component extends BaseComponent {
        String SELECTOR = "selector";
        String INSERTOR = "insertor";
        String SEQUNCER = "sequencer";
        String EXECUTOR = "executor";
    }

    interface Constant {
        int FREQUENCY_MICRO = 100;
        int VALUE_LEN = 32;// 32 bytes --> one cache line.
    }
}
