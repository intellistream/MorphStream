package common.constants;

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
        String Gate_Threads = "ed.gate.threads";
    }

    interface Component extends BaseConstants.BaseComponent {
        String SELECTOR = "selector";
        String INSERTOR = "insertor";
        String SEQUNCER = "sequencer";
        String EXECUTOR = "executor";
        String SPOUT = "spout"; //TODO: Verify this
        String TR = "TR";
        String WU = "WU";
        String TC = "TC";
        String TCG = "TCG";
        String SC = "SC";
        String CU = "CU";
        String ES = "ES";
        String SINK = "sink";
    }

    interface Constant {
        int FREQUENCY_MICRO = 100;
        int VALUE_LEN = 32;// 32 bytes --> one cache line.
    }
}
