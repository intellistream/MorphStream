package application.constants;

public interface LogProcessingConstants extends BaseConstants {
    String PREFIX = "lg";
    int max_hz = 50000;

    interface Field extends BaseField {
        String IP = "ip";
        String TIMESTAMP = "timestamp";
        String TIMESTAMP_MINUTES = "timestampMinutes";
        String REQUEST = "request";
        String RESPONSE = "response";
        String BYTE_SIZE = "byteSize";
        String COUNT = "count";
        String COUNTRY = "country";
        String COUNTRY_NAME = "country_name";
        String CITY = "city";
        String COUNTRY_TOTAL = "countryTotal";
        String CITY_TOTAL = "cityTotal";
    }

    interface Conf extends BaseConf {
        String VOLUME_COUNTER_WINDOW = "lg.volume_counter.window";
        String VOLUME_COUNTER_THREADS = "lg.volume_counter.threads";
        String STATUS_COUNTER_THREADS = "lg.status_counter.threads";
        String GEO_FINDER_THREADS = "lg.geo_finder.threads";
        String GEO_STATS_THREADS = "lg.geo_stats.threads";
    }

    interface Component extends BaseComponent {
        String VOLUME_COUNTER = "volumeCounterOneMin";
        String VOLUME_SINK = "countSink";
        String STATUS_COUNTER = "statusCounter";
        String STATUS_SINK = "statusSink";
        String GEO_FINDER = "geoFinder";
        String GEO_STATS = "geoStats";
        String GEO_SINK = "geoSink";
    }

    interface TunedConfiguration {
        int GEO_FINDER_THREADS_core1 = 1;
        int GEO_FINDER_THREADS_core2 = 1;
        int GEO_FINDER_THREADS_core4 = 1;
        int GEO_FINDER_THREADS_core8 = 4;
        int GEO_FINDER_THREADS_core16 = 1;
        int GEO_FINDER_THREADS_core32 = 2;

        int GEO_FINDER_THREADS_core8_Batch2 = 4;
        int GEO_FINDER_THREADS_core8_Batch4 = 4;
        int GEO_FINDER_THREADS_core8_Batch8 = 4;

        int acker_core8_Batch2 = 6;
        int acker_core8_Batch4 = 1;
        int acker_core8_Batch8 = 1;

        int GEO_FINDER_THREADS_core8_HP = 16;
        int GEO_FINDER_THREADS_core32_HP_Batch = 4;

        int acker_core1 = 1;
        int acker_core2 = 1;
        int acker_core4 = 1;
        int acker_core8 = 4;
        int acker_core16 = 1;
        int acker_core32 = 2;
        int acker_core8_HP = 16;

        int acker_core32_HP_Batch = 4;
    }
}
