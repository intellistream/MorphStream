package common.constants;

public interface NonGrepSumConstants extends BaseConstants {
    String PREFIX = "non_gs";
    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }
    interface Conf extends BaseConf {
        String Executor_Threads = "non_gs.executor.threads";
    }
}