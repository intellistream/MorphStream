package application.constants;

public interface PositionKeepingConstants extends BaseConstants {
    String PREFIX = "pk";

    interface Field {
        String DEVICE_ID = "device_Id";
    }

    interface Conf extends BaseConf {
        String PK_THREADS = "pk.transaction.threads";
    }

    interface Component extends BaseComponent {
        String PK = "PKBolt";
    }

    interface Constant {
        int SIZE_EVENT = 30;
        int SIZE_VALUE = 50;//vary its computing complexity.
        int NUM_MACHINES = 40;
        int MOVING_AVERAGE_WINDOW = 1_000;
        double SpikeThreshold = 0.3;
    }
}
