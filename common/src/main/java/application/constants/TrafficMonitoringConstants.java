package application.constants;

public interface TrafficMonitoringConstants extends BaseConstants {
    String PREFIX = "tm";
    int max_hz = 120000;

    interface Conf extends BaseConf {

        String MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile";
        String MAP_MATCHER_THREADS = "tm.map_matcher.threads";
        String MAP_MATCHER_LAT_MIN = "tm.map_matcher.lat.min";
        String MAP_MATCHER_LAT_MAX = "tm.map_matcher.lat.max";
        String MAP_MATCHER_LON_MIN = "tm.map_matcher.lon.min";
        String MAP_MATCHER_LON_MAX = "tm.map_matcher.lon.max";

        String SPEED_CALCULATOR_THREADS = "tm.speed_calculator.threads";

        String ROAD_FEATURE_ID_KEY = "tm.road.feature.id_key";
        String ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key";
    }

    interface Component extends BaseComponent {
        String MAP_MATCHER = "mapMatcherBolt";
        String SPEED_CALCULATOR = "speedCalculatorBolt";
    }

    interface Field extends BaseField {
        String VEHICLE_ID = "vehicleID";
        String DATE_TIME = "dateTime";
        String OCCUPIED = "occupied";
        String SPEED = "speed";
        String BEARING = "bearing";
        String LATITUDE = "latitude";
        String LONGITUDE = "longitude";
        String ROAD_ID = "roadID";
        String NOW_DATE = "nowDate";
        String AVG_SPEED = "averageSpeed";
        String COUNT = "count";
    }

    interface TunedConfiguration {
        int acker_core1 = 1;
        int acker_core2 = 1;
        int acker_core4 = 1;
        int acker_core8 = 1;
        int acker_core16 = 1;
        int acker_core32 = 1;
        int acker_core8_HP = 1;
        int acker_core8_Batch = 1;
        int acker_core32_HP_Batch = 1;

        int MAP_MATCHER_THREADS_core1 = 1;
        int SPEED_CALCULATOR_THREADS_core1 = 1;
        int MAP_MATCHER_THREADS_core2 = 1;
        int SPEED_CALCULATOR_THREADS_core2 = 1;
        int MAP_MATCHER_THREADS_core4 = 1;
        int SPEED_CALCULATOR_THREADS_core4 = 1;

        int MAP_MATCHER_THREADS_core8 = 8;
        int SPEED_CALCULATOR_THREADS_core8 = 2;

        int MAP_MATCHER_THREADS_core16 = 1;
        int SPEED_CALCULATOR_THREADS_core16 = 1;
        int MAP_MATCHER_THREADS_core32 = 32;
        int SPEED_CALCULATOR_THREADS_core32 = 4;

        int MAP_MATCHER_THREADS_core8_HP = 8;
        int SPEED_CALCULATOR_THREADS_core8_HP = 1;

        int MAP_MATCHER_THREADS_core8_Batch2 = 16;
        int SPEED_CALCULATOR_THREADS_core8_Batch2 = 16;
        int MAP_MATCHER_THREADS_core8_Batch4 = 16;
        int SPEED_CALCULATOR_THREADS_core8_Batch4 = 2;
        int MAP_MATCHER_THREADS_core8_Batch8 = 16;
        int SPEED_CALCULATOR_THREADS_core8_Batch8 = 4;

        int MAP_MATCHER_THREADS_core32_HP_Batch = 32;
        int SPEED_CALCULATOR_THREADS_core32_HP_Batch = 32;

    }
}
