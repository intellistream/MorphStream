package common.constants;

public interface IBWJConstants extends BaseConstants {
    String PREFIX = "mb";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConf {
        String INSERTOR_THREADS = "mb.insert.threads";
        String SELECTOR_THREADS = "mb.selector.threads";
        String Executor_Threads = "mb.executor.threads";
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
