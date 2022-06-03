package common.constants;

//TODO: Modify ED Constants. The current constants are copied from GrepSumConstants

public interface EventDetectionConstants {
    String PREFIX = "ed";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConstants.BaseConf {
        String INSERTOR_THREADS = "ed.insert.threads";
        String SELECTOR_THREADS = "ed.selector.threads";
        String Executor_Threads = "ed.executor.threads";
    }

    interface Component extends BaseConstants.BaseComponent {
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
