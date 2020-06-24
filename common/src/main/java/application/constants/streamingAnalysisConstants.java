package application.constants;

public interface streamingAnalysisConstants extends BaseConstants {
    String PREFIX = "sa";
    int max_hz = 120000;

    interface Field extends BaseField {
        String TIME = "time";
        String KEY = "key";
        String VALUE = "value";

    }

    interface Conf extends BaseConf {
        String EXECUTOR_THREADS1 = "threads1";
        String EXECUTOR_THREADS2 = "threads2";
        String EXECUTOR_THREADS3 = "threads3";
        String EXECUTOR_THREADS4 = "threads4";
    }

    interface Component extends BaseComponent {
        String FILTER = "filter";
        String MEDIAN = "median";
        String RANK = "rank";

        String MEDIAN2 = "median2";
        String RANK2 = "rank2";
    }
}
