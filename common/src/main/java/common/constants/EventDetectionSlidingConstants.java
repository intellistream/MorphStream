package common.constants;

public interface EventDetectionSlidingConstants {
    String PREFIX = "eds";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConstants.BaseConf {
        String INSERTOR_THREADS = "eds.insert.threads";
        String SELECTOR_THREADS = "eds.selector.threads";
        String Executor_Threads = "eds.executor.threads";
        String Gate_Threads = "eds.gate.threads";
    }

    interface Component extends BaseConstants.BaseComponent {
        String SELECTOR = "selector";
        String INSERTOR = "insertor";
        String SEQUNCER = "sequencer";
        String EXECUTOR = "executor";
        String SPOUT = "spout"; //TODO: Verify this
        String TRS = "TRS";
        String WUS = "WUS";
        String TCS = "TCS";
        String TCGS = "TCGS";
        String SCS = "SCS";
        String CUS = "CUS";
        String ESS = "ESS";
        String SINK = "sink";
    }

    interface Constant {
        int FREQUENCY_MICRO = 100;
        int VALUE_LEN = 32;// 32 bytes --> one cache line.
    }
}
