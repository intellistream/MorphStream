package common.constants;

/**
 * @author mayconbordin
 */
public interface TPConstants extends BaseConstants {
    String PREFIX = "tptxn";
    int max_hz = 550000;
    String CONFIG_FILENAME = "LinearRoad.properties";
    String LINEAR_HISTORY = "linear-history-file";
    String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    long POS_EVENT_TYPE = 0;
    long ACC_BAL_EVENT_TYPE = 2;
    long DAILY_EXP_EVENT_TYPE = 3;
    long TRAVELTIME_EVENT_TYPE = 4;
    long NOV_EVENT_TYPE = -5;
    long LAV_EVENT_TYPE = -6;
    long TOLL_EVENT_TYPE = 7;
    long ACCIDENT_EVENT_TYPE = -8;
    String LINEAR_DB_HOST = "linear-db-host";
    String LINEAR_DB_PORT = "linear-db-port";
    int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    String CLEAN_START = "clean-start";
    String HISTORY_COMPONENT_HOST = "localhost";

    interface Constant {
        int NUM_SEGMENTS = 100;
        int MAX_SPEED = 200;
        int MAX_INT = 200;
    }

    interface Field extends BaseField {
        String TIMESTAMP = "timestamp";
        String VEHICLE_ID = "vehicleId";
        String SPEED = "speed";
        String EXPRESSWAY = "expressway";
        String LANE = "lane";
        String DIRECTION = "direction";
        String SEGMENT = "segment";
        String POSITION = "position";
    }

    //get the correct location of the history loading component.
    interface Conf extends BaseConf {
        String segstatBoltThreads = "tp.segstat.threads";
        String AccidentDetectionBoltThreads = "tp.accident.threads";
        String tollBoltThreads = "tp.toll.threads";
        String toll_cv_BoltThreads = "tp.toll.cv.threads";
        String toll_las_BoltThreads = "tp.toll.las.threads";
        String toll_pos_BoltThreads = "tp.toll.pos.threads";
        String dailyExpBoltThreads = "tp.dailyExp.threads";
        String xways = "lr.xways";
        String DispatcherBoltThreads = "tp.dispatch.threads";
        String LatestAverageVelocityThreads = "tp.latest.threads";
        String AccidentNotificationBoltThreads = "tp.accidentnoti.threads";
        String AccountBalanceBoltThreads = "tp.accno.threads";
        String AverageVehicleSpeedThreads = "tp.average.vehicle.threads";
        String AverageSpeedThreads = "tp.average.threads";
        String COUNT_VEHICLES_Threads = "tp.count.threads";
    }

    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }
}
